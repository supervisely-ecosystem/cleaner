import os
from datetime import datetime, timedelta
import supervisely_lib as sly
from supervisely.api.user_api import UserApi
from dotenv import load_dotenv


if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))


path_to_del = "/tmp/supervisely/export"
possible_pathes_to_del = [
    "/Export-as-masks",
    "/Export-to-Supervisely",
    "/yolov5_format",
    "/Export to COCO",
    "/ApplicationsData/Export-to-Pascal-VOC",
    "/activity_data",
    "/reference_items",
    "/Export only labeled items",
    "/ApplicationsData/Export-Metadata",
    "/cityscapes_format",
    "/video_from_images",
    "/tags_to_urls",
    "/export-to-dota",
]
days_storage = 30
del_date = datetime.now() - timedelta(days=days_storage)


def sort_by_date(my_files_info):
    file_to_del_pathes = list(
        filter(
            None,
            map(
                lambda file_info: file_info["path"]
                if datetime.strptime(file_info["updatedAt"].split("T")[0], "%Y-%m-%d") < del_date
                else [],
                my_files_info,
            ),
        )
    )
    return file_to_del_pathes


def main():

    api = sly.Api.from_env()
    logger = api.logger
    # logger.setLevel(5) # set trace logger

    teams_infos = api.team.get_list()
    for team_info in teams_infos:
        team_id = team_info[0]
        team_name = team_info[1]
        logger.info("Check old files for {} team".format(team_name))

        files_info = api.file.list(team_id, path_to_del)
        file_to_del_pathes = sort_by_date(files_info)

        for curr_path_to_del in possible_pathes_to_del:
            files_info_old = api.file.list(team_id, curr_path_to_del)
            file_to_del_pathes.extend(sort_by_date(files_info_old))

        for curr_file_path in file_to_del_pathes:
            logger.trace("Delete file: {}".format(curr_file_path))
            api.file.remove(team_id, curr_file_path)


if __name__ == "__main__":
    sly.main_wrapper("main", main)
