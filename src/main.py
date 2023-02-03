import os, time
from datetime import datetime, timedelta
from distutils.util import strtobool
from dotenv import load_dotenv
from functools import partial

import supervisely as sly

if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

api = sly.Api.from_env()

path_to_del = "/tmp/supervisely/export"
offlines_path = "/offline-sessions/"  # offline sessoins files for apps with GUI 2.0
possible_paths_to_del = [
    # https://github.com/supervisely-ecosystem/export-as-masks
    "/Export-as-masks",
    # https://github.com/supervisely-ecosystem/export-to-supervisely-format
    "/Export-to-Supervisely",
    # https://github.com/supervisely-ecosystem/convert-supervisely-to-yolov5-format
    "/yolov5_format",
    # https://github.com/supervisely-ecosystem/export-to-coco
    "/Export to COCO",
    # https://github.com/supervisely-ecosystem/export-to-pascal-voc
    "/ApplicationsData/Export-to-Pascal-VOC",
    # https://github.com/supervisely-ecosystem/export-activity-as-csv
    "/activity_data",
    # https://github.com/supervisely-ecosystem/create-json-with-reference-items
    "/reference_items",
    # https://github.com/supervisely-ecosystem/export-only-labeled-items
    "/Export only labeled items",
    # https://github.com/supervisely-ecosystem/export-metadata
    "/ApplicationsData/Export-Metadata",
    # https://github.com/supervisely-ecosystem/export-to-cityscapes
    "/cityscapes_format",
    # https://github.com/supervisely-ecosystem/render-video-from-images
    "/video_from_images",
    # https://github.com/supervisely-ecosystem/tags-to-image-urls
    "/tags_to_urls",
    # https://github.com/supervisely-ecosystem/export-to-dota
    "/export-to-dota",
]

all_teams = bool(strtobool(os.getenv('modal.state.allTeams')))
selected_team_id = None
if all_teams is False:
    selected_team_id = int(os.environ["modal.state.teamId"])
days_storage = int(os.environ["modal.state.clear"])
sleep_days = int(os.environ["modal.state.sleep"])
sleep_time = sleep_days * 86400
del_date = datetime.now() - timedelta(days=days_storage)


def sort_by_date_and_ext(files_info, offline_sessions: bool = False):
    file_to_del_paths = []
    extensions_to_delete = [".py", ".pyc", ".md", ".sh"]

    for file_info in files_info:
        file_date_str = file_info["updatedAt"].split("T")[0]
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
        file_ext = os.path.splitext(os.path.basename(file_info["name"]))[1]

        if file_date < del_date:
            file_to_del_paths.append(file_info["path"])
        elif offline_sessions and file_ext in extensions_to_delete:
            file_to_del_paths.append(file_info["path"])

    return file_to_del_paths


def update_progress(count, api: sly.Api, progress: sly.Progress):
    count = min(count, progress.total - progress.current)
    progress.iters_done(count)
    if progress.need_report():
        progress.report_progress()


def get_progress_cb(api, message, total, is_size=False, func=update_progress):
    progress = sly.Progress(message, total, is_size=is_size)
    progress_cb = partial(func, api=api, progress=progress)
    progress_cb(0)
    return progress_cb


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
            team_id = team_info[0]
            team_name = team_info[1]
            sly.logger.info("Check old files for {} team".format(team_name))

            sly.logger.info("Checking files in {}. Team: {}".format(path_to_del, team_name))
            files_info = api.file.list(team_id, path_to_del)
            file_to_del_paths = sort_by_date_and_ext(files_info)

            for curr_path in possible_paths_to_del:
                sly.logger.info("Checking files in {}. Team: {}".format(curr_path, team_name))
                files_info_old = api.file.list(team_id, curr_path)
                file_to_del_paths.extend(sort_by_date_and_ext(files_info_old))

            sly.logger.info("Checking files in {}; this may take a moment".format(offlines_path))
            offline_sessions_infos = api.file.listdir(team_id, offlines_path)
            for path in offline_sessions_infos:
                session_files = api.file.list(team_id, path)
                file_to_del_paths.extend(sort_by_date_and_ext(session_files, offline_sessions=True))

            sly.logger.info("Start removing. Team: {}".format(team_name))
            progress_cb = get_progress_cb(api, "Removing files", len(file_to_del_paths))
            api.file.remove_batch(team_id, file_to_del_paths, progress_cb)

            total_files_cnt += len(file_to_del_paths)
            sly.logger.info("Total removed {} files. Team: {}".format(total_files_cnt, team_name))
            progress.message = "Total removed {} files. Team: {}".format(total_files_cnt, team_name)
            progress.iter_done_report()
            time.sleep(2)

        sleep_text = f"{sleep_days} day" if sleep_days <= 1 else f"{sleep_days} days"
        sly.logger.info("Finished. Sleep time: {}.".format(sleep_text))
        progress.message = "Finished. Sleep time: {}.".format(sleep_text)
        progress.print_progress()
        time.sleep(sleep_time)


if __name__ == "__main__":
    sly.main_wrapper("main", main)
