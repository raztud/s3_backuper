# S3 Backuper
Backup a structure of directories to S3

The script uses [AWS CLI](https://aws.amazon.com/cli/) to upload a structure of folders in S3. 
The script supports multithread in order to have paralel uploads. 

Run as:
```
python cli_backuper.py --folder=/home/user/photos/ --s3path=/backup/photos/
```
