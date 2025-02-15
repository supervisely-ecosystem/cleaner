import base64
from typing import List

from datetime import datetime
from typing import List
import requests
import json
import supervisely as sly
from supervisely.api.module_api import ApiField
from supervisely.api.storage_api import StorageApi
from supervisely.api.file_api import FileInfo
from typing import List, Union, Dict, Optional, Literal
from supervisely import tqdm_sly

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
