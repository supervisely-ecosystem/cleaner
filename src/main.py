import os, time
from datetime import datetime, timedelta
import supervisely_lib as sly
from dotenv import load_dotenv


if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))


api = sly.Api.from_env()
path_to_del = "/tmp/supervisely/export"
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
days_storage = 30
del_date = datetime.now() - timedelta(days=days_storage)
sleep_time = 86400
gb_format = 1024 * 1024 * 1024


def sort_by_date(files_info):
    file_to_del_paths = []
    for file_info in files_info:
        file_date_str = file_info["updatedAt"].split("T")[0]
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
        if file_date < del_date:
            file_to_del_paths.append(file_info["path"])

    return file_to_del_paths


def main():

    while True:
        total_size = 0
        total_files_cnt = 0
        teams_infos = api.team.get_list()
        progress = sly.Progress("Start cleaning", len(teams_infos))
        for team_info in teams_infos:
            team_id = team_info[0]
            files_info = api.file.list(team_id, path_to_del)
            file_to_del_paths = sort_by_date(files_info)
            for curr_path_to_del in possible_paths_to_del:
                files_info_old = api.file.list(team_id, curr_path_to_del)
                file_to_del_paths.extend(sort_by_date(files_info_old))

            for curr_file_path in file_to_del_paths:
                curr_size = api.file.get_directory_size(team_id, curr_file_path)
                total_size += curr_size
                total_files_cnt += 1
                api.file.remove(team_id, curr_file_path)
            progress.message = "Total removed {} files ({} Gb). Team: ".format(
                total_files_cnt, round(total_size / gb_format, 4)
            )
            progress.iter_done_report()

        time.sleep(sleep_time)


if __name__ == "__main__":
    sly.main_wrapper("main", main)
