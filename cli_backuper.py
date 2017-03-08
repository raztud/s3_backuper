import os
import argparse
import sys
import threading
import logging
import subprocess

is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue
else:
    import queue as Queue


BUCKET = '<BUCKET_NAME>'  # the bucket name where to upload
PROFILE = 'default'  # the profile name
REGION = 'eu-central-1'

config = {
	'number_of_threads': 10,
}

logging.basicConfig(filename='backuper.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def get_file_list(folders):
	"""
	:type folders: list
	"""
	fileslist = []
	total = 0
	for rootDir in folders:
		for dirName, subdirList, fileList in os.walk(rootDir):
		# print('Found directory: %s' % dirName)
			for fname in fileList:
				if fname == 'Thumbs.db':
					continue

				if fname[-3:].lower() == 'thm':
					continue

				path = os.path.join(dirName, fname)
				st = os.stat(path)
				total += st.st_size
				fileslist.append(path)

	return fileslist, total

def upload(filename, remaining, total_files):
	s3file = s3path + filename.replace(folder, '')

	try:
		res = subprocess.check_output(['aws', 's3', 'ls', 's3://{bucket}/{s3path}'.format(bucket=BUCKET, s3path=s3file), '--region={region}'.format(region=REGION), '--profile={profile}'.format(profile=PROFILE)])
		logging.info("SKIP ({filecount}/{total}): {filename}".format(filename=filename, filecount=total_files-remaining, total=total_files))
	except:
		logging.info("UPLOAD ({filecount}/{total}): {filename} -> {s3file}".format(filecount=total_files-remaining, total=total_files, filename=filename, s3file=s3file))
		try:
			subprocess.check_output(['aws', 's3', 'cp', '{filename}'.format(filename=filename), 's3://{bucket}/{s3path}'.format(bucket=BUCKET, s3path=s3file),
				'--region={region}'.format(region=REGION), '--profile={profile}'.format(profile=PROFILE),])
		except:
			logging.error("Could not upload {}".format(filename))

def worker(queue, total_files):
    queue_full = True
    while queue_full:
        try:
            fpath = queue.get(False)
            upload(fpath, queue.qsize(), total_files)
        except Queue.Empty:
            queue_full = False

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--folder', help='Folder to backup', required=True)
	parser.add_argument('--s3path', help='The path in S3 where to be written. Eg: /backup/')
	args = parser.parse_args()

	folder = args.folder
	if os.path.isdir(folder):
		if folder[-1] != os.sep:
			folder += os.sep

	logging.info("Uploading folder {}".format(folder))

	s3path = args.s3path
	if not s3path:
		# TODO
		s3path = folder.replace(folder, '')

	if s3path[-1] != '/':
		s3path += '/'

	logging.info("Backuper started. Getting file list...")
	filelist, total = get_file_list((folder,))
	total_files = len(filelist)

	logging.info("Total size: {} MB".format(round(total / 1024 / 1024, 2)))
	
	q = Queue.Queue()
	for file_path in filelist:
		q.put(file_path)

	thread_count = config.get('number_of_threads', 5)
	logging.info("Sync started with {} threads".format(thread_count))

	for i in range(thread_count):
		t = threading.Thread(target=worker, args = (q, total_files))
		t.start()
        
