import asyncio
import base64
import json
import time
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

import requests
import supervisely as sly
from supervisely import tqdm_sly
from supervisely.api.file_api import FileInfo
from supervisely.api.module_api import ApiField
from supervisely.api.storage_api import StorageApi

DEFAULT_LIMIT = 10000

def sort_by_date(files_info: List[dict], del_date: datetime) -> List[str]:
    file_to_del_paths = []

    for file_info in files_info:
        file_date_str = file_info["updatedAt"].split("T")[0]
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

        if file_date < del_date:
            file_to_del_paths.append(file_info["path"])

    return file_to_del_paths


class CustomStorageApi(StorageApi):
    """Custom implementation of the StorageApi class for Cleaner."""

    def list(
        self,
        team_id: int,
        path: str,
        recursive: bool = True,
        return_type: Literal["dict", "fileinfo"] = "fileinfo",
        with_metadata: bool = True,
        include_files: bool = True,
        include_folders: bool = True,
        limit: Optional[int] = None,
        continuation_token: Optional[str] = None,  # * custom argument
    ) -> List[Union[Dict, FileInfo]]:
        """Custom implementation of the list method."""

        if not path.endswith("/"):
            path += "/"
        method = "file-storage.v2.list"
        json_body = {
            ApiField.TEAM_ID: team_id,
            ApiField.PATH: path,
            ApiField.RECURSIVE: recursive,
            ApiField.WITH_METADATA: with_metadata,
            ApiField.FILES: include_files,
            ApiField.FOLDERS: include_folders,
        }
        if limit is not None:
            json_body[ApiField.LIMIT] = limit

        try:
            data = []
            limit_exceeded = False
            if continuation_token is None:
                # * get first response only if continuation_token is None
                first_response = self._api.post(method, json_body).json()
                data = first_response.get("entities", [])

                continuation_token = first_response.get("continuationToken", None)
                if limit is not None and len(data) >= limit:
                    limit_exceeded = True

            if continuation_token is None or limit_exceeded:
                pass
            else:
                while continuation_token is not None:
                    json_body["continuationToken"] = continuation_token
                    temp_resp = self._api.post(method, json_body)
                    temp_data = temp_resp.json().get("entities", [])
                    data.extend(temp_data)
                    continuation_token = temp_resp.json().get("continuationToken", None)
                    if limit is not None and len(data) >= limit:
                        limit_exceeded = True
                        break

        except requests.exceptions.RequestException as e:
            if self.is_on_agent(path) is True:
                sly.logger.warning(
                    f"Failed to list files on agent {path}: {repr(e)}", exc_info=True
                )
                return []
            else:
                raise e
        if limit_exceeded:
            data = data[:limit]

        if return_type == "fileinfo":
            results = []
            for info in data:
                info[ApiField.IS_DIR] = info[ApiField.TYPE] == "folder"
                results.append(self._convert_json_info(info))
            return results

        return data


def path_to_base64(path: str) -> str:
    return base64.b64encode(path.encode()).decode()


def get_task_id(path):
    return int(path.split("/")[2])


def is_removable(task_info, apps):
    return task_info["meta"]["app"]["name"] in apps


def should_delete_file(file_info: dict) -> bool:
    return sly.fs.get_file_ext(file_info["name"]) in [".py", ".pyc", ".md", ".sh"]


def clean_offline_sessions(
    api: sly.Api,
    team_id: int,
    offlines_path: str,
    app_names: List[str],
    batch_size: int = 20000,
    w_ids=None,
):
    """Clean offline sessions files."""
    sly.logger.info(f"Start cleaning offline sessions files (batch size: {batch_size})")

    # * custom implementation of the list method for Cleaner
    api.storage = CustomStorageApi(api)

    if w_ids is None:
        w_ids = [workspace.id for workspace in api.workspace.get_list(team_id)]

    removed_files = 0
    batch_num = 1
    task_ids_to_remove = set()
    continuation_token = None
    scanned_files = 0

    while True:

        last_file = None
        try:
            files_infos = api.storage.list(
                team_id,
                offlines_path,
                return_type="dict",
                include_folders=False,
                with_metadata=False,
                limit=batch_size,
                continuation_token=continuation_token,
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "limit" in e.response.text:
                sly.logger.warning(f"Failed to list storage files due to limit error. Checking max limit...")
                try:
                    error_details = json.loads(e.response.text)
                    max_limit = error_details["details"][0]["context"]["limit"]
                    sly.logger.info(f"Max limit for storage listing set to {max_limit} due to API response recommendation.")
                except Exception as e:
                    sly.logger.warning(f"Failed to get max limit, setting default: {DEFAULT_LIMIT}")
                    max_limit = DEFAULT_LIMIT
                batch_size = max_limit
                try:
                    files_infos = api.storage.list(
                        team_id,
                        offlines_path,
                        return_type="dict",
                        include_folders=False,
                        with_metadata=False,
                        limit=batch_size,
                        continuation_token=continuation_token,
                    )
                except Exception as e:
                    sly.logger.warning(f"Failed to list files after adjusting limit: {repr(e)}")
            else:
                sly.logger.warning(f"Failed to list files: {repr(e)}")
        except Exception as e:
            sly.logger.warning(f"Failed to list files: {repr(e)}")
            
        scanned_files += len(files_infos)

        all_task_ids = list(set([get_task_id(file_info["path"]) for file_info in files_infos]))
        if all_task_ids:
            if w_ids:
                task_infos = []
                for batch_tasks in sly.batched(all_task_ids, 500):
                    filters = [{"field": "id", "operator": "in", "value": batch_tasks}]
                    task_infos.extend([t for w_id in w_ids for t in api.task.get_list(w_id, filters)])
            else:
                task_infos = []
        else:
            task_infos = []

        if len(task_infos) > 0:
            task_ids_to_remove.update({t["id"] for t in task_infos if is_removable(t, app_names)})
        
        file_to_del_paths = []
        for file_info in files_infos:
            if get_task_id(file_info["path"]) in task_ids_to_remove:
                file_to_del_paths.append(file_info["path"])
                continue
            elif should_delete_file(file_info):
                file_to_del_paths.append(file_info["path"])
                continue
            last_file = file_info["path"]

        continuation_token = path_to_base64(last_file) if last_file else None

        curr_batch_len = len(file_to_del_paths)
        if curr_batch_len > 0:
            pbar = tqdm_sly(
                desc=f"Removing batch {batch_num}", total=curr_batch_len
            ).update
            api.file.remove_batch(team_id, file_to_del_paths, pbar, batch_size)
            removed_files += curr_batch_len
            sly.logger.info(f"Batch {batch_num} finished. Removed: {curr_batch_len}")
            batch_num += 1

        if len(files_infos) < batch_size:
            break

    sly.logger.info(f"Total files scanned in offline sessions: {scanned_files}")
    return removed_files

async def teams_get_list_async(
    api: sly.Api,
    filters: List[Dict[str, str]] = None,
    limit: int = None,
):
    """
    Get list of teams asynchronously from the Supervisely server.
    """
    method = "teams.list"
    data = {
        ApiField.FILTER: filters or [],
        ApiField.SORT: ApiField.ID,
        ApiField.SORT_ORDER: "asc"
    }
    
    semaphore = asyncio.Semaphore(5)
    pages_count = None
    tasks: List[asyncio.Task] = []

    async def _r(data_, page_num):
        nonlocal pages_count
        async with semaphore:
            response = await api.post_async(method, data_)
            response_json = response.json()
            items = response_json.get("entities", [])
            pages_count = response_json["pagesCount"]
        return [api.team._convert_json_info(item) for item in items]

    # Get first page
    data[ApiField.PAGE] = 1
    t = time.monotonic()
    items = await _r(data, 1)
    sly.logger.debug(f"Awaited teams page 1/{pages_count} for {time.monotonic() - t:.4f} sec")
    
    # Check if we've exceeded the limit with just the first page
    if limit is not None and len(items) >= limit:
        return items[:limit]
    
    # Get remaining pages in parallel
    t = time.monotonic()
    for page_n in range(2, pages_count + 1):
        data[ApiField.PAGE] = page_n
        tasks.append(asyncio.create_task(_r(data.copy(), page_n)))
    
    # Await all tasks and collect results
    for i, task in enumerate(tasks, 2):
        new_items = await task
        items.extend(new_items)
        sly.logger.debug(f"Awaited teams page {i}/{pages_count} for {time.monotonic() - t:.4f} sec")
        t = time.monotonic()
        
        # Check if we've exceeded the limit
        if limit is not None and len(items) >= limit:
            return items[:limit]
    
    return items

async def storage_get_list_async(
    api: sly.Api,
    team_id: int,
    path: str,
    recursive: bool = True,
    return_type: Literal["dict", "fileinfo"] = "fileinfo",
    with_metadata: bool = True,
    include_files: bool = True,
    include_folders: bool = True,
    limit: Optional[int] = None,
):
    """
    List files asynchronously from the Team Files or Cloud Storages.
    """
    if not path.endswith("/"):
        path += "/"
    method = "file-storage.v2.list"
    json_body = {
        ApiField.TEAM_ID: team_id,
        ApiField.PATH: path,
        ApiField.RECURSIVE: recursive,
        ApiField.WITH_METADATA: with_metadata,
        ApiField.FILES: include_files,
        ApiField.FOLDERS: include_folders,
    }
    if limit is not None:
        json_body[ApiField.LIMIT] = limit

    semaphore = asyncio.Semaphore(5)
    tasks = []
    all_data = []
    
    async def _fetch_data(token=None):
        nonlocal json_body
        req_data = json_body.copy()
        if token:
            req_data["continuationToken"] = token
        
        async with semaphore:
            t = time.monotonic()
            response = await api.post_async(method, req_data)
            response_json = response.json()
            entities = response_json.get("entities", [])
            token = response_json.get("continuationToken", None)
            sly.logger.debug(f"Fetched {len(entities)} files in {time.monotonic() - t:.4f} sec")
            return entities, token

    # Get first batch of data
    t_total = time.monotonic()
    entities, continuation_token = await _fetch_data()
    all_data.extend(entities)

    # Check if we've exceeded the limit with just the first batch
    if limit is not None and len(all_data) >= limit:
        all_data = all_data[:limit]
        continuation_token = None
    
    # Process remaining data in parallel batches
    while continuation_token:
        next_batch_tokens = []
        current_token = continuation_token
        continuation_token = None
        
        # Create initial task for the current token
        tasks.append(asyncio.create_task(_fetch_data(current_token)))
        
        # Wait for all tasks to complete
        for task in asyncio.as_completed(tasks):
            entities, token = await task
            all_data.extend(entities)
            if token:
                next_batch_tokens.append(token)
            
            # Check if we've exceeded the limit
            if limit is not None and len(all_data) >= limit:
                all_data = all_data[:limit]
                next_batch_tokens = []
                break
        
        # Clear tasks for next batch
        tasks = []
        
        # Set continuation token for next iteration if we have more tokens
        if next_batch_tokens:
            continuation_token = next_batch_tokens[0]
            for token in next_batch_tokens[1:]:
                tasks.append(asyncio.create_task(_fetch_data(token)))
    
    sly.logger.debug(f"Total file listing completed in {time.monotonic() - t_total:.4f} sec, fetched {len(all_data)} files")
    
    # Convert results if needed
    if return_type == "fileinfo":
        results = []
        for info in all_data:
            info[ApiField.IS_DIR] = info[ApiField.TYPE] == "folder"
            results.append(api.storage._convert_json_info(info))
        return results

    return all_data