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
]
days_storage = 30
del_date = datetime.now() - timedelta(days=days_storage)


def main():

    api = sly.Api.from_env()
    logger = api.logger

    my_files_info = api.file.list(439, path_to_del)
    my_files_info2 = api.file.list(443, path_to_del)

    test = [my_files_info, my_files_info2]

    teams_infos = api.team.get_list()
    for team_info in teams_infos:
        team_id = team_info[0]
        team_name = team_info[1]
        files_info = api.file.list(team_id, path_to_del)
        logger.warn("Check old files for {} team".format(team_name))
        for file_info in files_info:
            file_date_str = file_info["updatedAt"].split("T")[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
            if file_date < del_date:
                file_path = file_info["path"]
                logger.trace("Delete file: {}".format(file_path))
                api.file.remove(team_id, file_path)


if __name__ == "__main__":
    sly.main_wrapper("main", main)
