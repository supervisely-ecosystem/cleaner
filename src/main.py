import os
import time
from datetime import datetime, timedelta
from distutils.util import strtobool
import sly_functions as f
from supervisely import tqdm_sly

import supervisely as sly
from dotenv import load_dotenv

if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

api = sly.Api.from_env()

# * list of apps to remove offline sessions files
apps_to_clean = [
    "On-the-Fly Quality Assurance",
    "Render previews GUI",
]

export_path_to_del = "/tmp/supervisely/export"  # export
import_path_to_del = "/import"  # import
offlines_path = "/offline-sessions/"  # offline sessoins files for apps with GUI 2.0
possible_paths_to_del = [
    "/Export-as-masks",  # https://github.com/supervisely-ecosystem/export-as-masks
    "/Export-to-Supervisely",  # https://github.com/supervisely-ecosystem/export-to-supervisely-format
    "/yolov5_format",  # https://github.com/supervisely-ecosystem/convert-supervisely-to-yolov5-format
    "/Export to COCO",  # https://github.com/supervisely-ecosystem/export-to-coco
    "/ApplicationsData/Export-to-Pascal-VOC",  # https://github.com/supervisely-ecosystem/export-to-pascal-voc
    "/activity_data",  # https://github.com/supervisely-ecosystem/export-activity-as-csv
    "/reference_items",  # https://github.com/supervisely-ecosystem/create-json-with-reference-items
    "/Export only labeled items",  # https://github.com/supervisely-ecosystem/export-only-labeled-items
    "/ApplicationsData/Export-Metadata",  # https://github.com/supervisely-ecosystem/export-metadata
    "/cityscapes_format",  # https://github.com/supervisely-ecosystem/export-to-cityscapes
    "/video_from_images",  # https://github.com/supervisely-ecosystem/render-video-from-images
    "/tags_to_urls",  # https://github.com/supervisely-ecosystem/tags-to-image-urls
    "/export-to-dota",  # https://github.com/supervisely-ecosystem/export-to-dota
]

all_teams = bool(strtobool(os.getenv("modal.state.allTeams")))
selected_team_id = None
if all_teams is False:
    selected_team_id = int(os.environ["modal.state.teamId"])
days_storage = int(os.environ.get("modal.state.clear", 30))
sleep_days = int(os.environ.get("modal.state.sleep", 2))
batch_size = int(os.environ.get("modal.state.batchSize", 20000))
sleep_time = sleep_days * 86400
del_date = datetime.now() - timedelta(days=days_storage)


def main():

    while True:
        total_files_cnt = 0
        teams_infos = None
        if all_teams is False and selected_team_id is not None:
            teams_infos = [api.team.get_info_by_id(selected_team_id)]
        else:
            teams_infos = api.team.get_list()
        progress = sly.Progress("Start cleaning", len(teams_infos))
        for team_info in teams_infos:
            team_id = team_info.id
            team_name = team_info.name

            workspaces = api.workspace.get_list(team_id)
            workspaces_ids = [workspace.id for workspace in workspaces]
            sly.logger.info(f"Check old files for {team_name} team")

            # export directory
            sly.logger.info(
                f"Team: {team_name}. Checking files in {export_path_to_del}."
            )
            files_info = api.storage.list(
                team_id,
                export_path_to_del,
                return_type="dict",
                include_folders=False,
                with_metadata=False,
            )
            file_to_del_paths = f.sort_by_date(files_info, del_date)

            # import directory
            sly.logger.info(
                f"Team: {team_name}. Checking files in {import_path_to_del}."
            )
            files_info = api.storage.list(
                team_id,
                import_path_to_del,
                return_type="dict",
                include_folders=False,
                with_metadata=False,
            )
            file_to_del_paths.extend(f.sort_by_date(files_info, del_date))

            # other legacy directories
            for curr_path in possible_paths_to_del:
                sly.logger.info(f"Team: {team_name}. Checking files in {curr_path}.")
                files_info_old = api.storage.list(
                    team_id,
                    curr_path,
                    return_type="dict",
                    include_folders=False,
                    with_metadata=False,
                )
                file_to_del_paths.extend(f.sort_by_date(files_info_old, del_date))

            if len(file_to_del_paths) > 0:
                sly.logger.info(f"Team: {team_name}. Start removing.")
                pbar = tqdm_sly(total=len(file_to_del_paths), desc="Cleaning...").update
                api.file.remove_batch(team_id, file_to_del_paths, pbar, batch_size)
                total_files_cnt += len(file_to_del_paths)

            # # offline sessions files
            removed_files = f.clean_offline_sessions(
                api, team_id, offlines_path, apps_to_clean, batch_size, workspaces_ids
            )
            total_files_cnt += removed_files

            sly.logger.info(f"Team: {team_name}. Total removed: {total_files_cnt}.")
            progress.message = f"Team: {team_name}. Total removed: {total_files_cnt}."
            progress.iter_done_report()
            time.sleep(2)

        sleep_text = f"{sleep_days} day" if sleep_days <= 1 else f"{sleep_days} days"
        sly.logger.info(f"Finished. Sleep time: {sleep_text}.")
        progress.message = f"Finished. Sleep time: {sleep_text}."
        progress.print_progress()
        time.sleep(sleep_time)


if __name__ == "__main__":
    sly.main_wrapper("main", main)
