<div align='center' markdown> 
<img src='https://user-images.githubusercontent.com/115161827/215558057-ca96a7e2-c243-4232-8133-8cab6b42d904.png'>

# Cleaner

<p align='center'>
  <a href='#overview'>Overview</a> •
   <a href='#How-to-Run'>How to Run</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervise.ly/apps/supervisely-ecosystem/cleaner)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.ly/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/cleaner?include_prereleases)
[![views](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/cleaner&counter=views&label=views)](https://supervise.ly)
[![used by teams](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/cleaner&counter=downloads&label=used%20by%20teams)](https://supervise.ly)
[![runs](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/cleaner&counter=runs&label=runs&123)](https://supervise.ly)

</div>

## Overview

This application allows admins to clear the teams folders for all teams in an instance over a period of time(in days).
Cleaning in teams folders will be carried out in the following paths:

- `/tmp/supervisely/export` - directory where temporary files are saved during export.
- `/offline-sessions` - directory where files that make apps sessions available after shutdown are saved.
- directories where temporary files could also be saved:
  - `/Export-as-masks`
  - `/Export-to-Supervisely`
  - `/yolov5_format`
  - `/Export to COCO`
  - `/ApplicationsData/Export-to-Pascal-VOC`
  - `/activity_data`
  - `/reference_items`
  - `/Export only labeled items`
  - `/ApplicationsData/Export-Metadata`
  - `/cityscapes_format`
  - `/video_from_images`
  - `/tags_to_urls`
  - `/export-to-dota`

Most of all these paths are used for exporting data and many users forget to empty these folders after they finish their work.
This is necessary to delete obsolete temporary data that is no longer used and takes up a lot of space on the instance.

After the cleaning is finished, the application will continue to run in the background and resume cleaning after a specified period of time (in days). You can stop it in `Workspace Tasks`.

## How to Run

1. Run app from the ecosystem.

<div align="center" markdown>
<img src="https://user-images.githubusercontent.com/79905215/217010352-0c1ea4a5-611d-4002-ac74-e92360fbcd68.png" width="650"/>
</div>

2. **Select team** whose files you want to clear (or choose `all teams`), **set period in days** (files older than this period will be deleted), **enter count of days to sleep** after cleaning before the next start and press the `RUN` button.

<div align="center" markdown>
<img src="https://user-images.githubusercontent.com/79905215/217009473-cd4ba9ec-bbd4-4971-94da-e9bf3582c2ba.png" width="650"/>
</div>
