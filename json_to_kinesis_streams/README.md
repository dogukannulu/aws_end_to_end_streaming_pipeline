# Overview

This process will be running inside an EC2 instance. Therefore, no AWS credential is defined or used inside the scripts. If you want to run locally or somewhere else, you have to add AWS credentials to the scripts.

This part of the repo is created for:

- We should create the Kinesis Data Stream from the AWS Console and define the function with the same exact stream name.
- The JSON file will be retrieved from a URL and will be streaming into the stream with predefined interval and length.
- The JSON won't be modified. If modification is required, you can add to the Python script.
- The script will be valid for the region `eu-central-1`, but you can modify the region part if necessary.

## Steps

1. We have to first create the working directory `/project`(We have to be located in `/`).
```
sudo mkdir /project
cd /project
```

2. First, we have to run this command to bring the `setup.sh` into the EC2 instance (We have to be located in `/`).
```
sudo curl -O https://raw.githubusercontent.com/dogukannulu/send_data_to_aws_services/main/json_to_kinesis_streams/setup.sh
```

3. The shell script runs the Python script with predefined command line arguments. You can modify lines 51-55 in `setup.sh` according to your use case with `sudo vi setup.sh`. To execute the shell script we should run the following command.
```
sudo chmod +x setup.sh
```

4. We should download the zip file that includes `json_to_kinesis_streams.py` and `requirements.txt`
```
sudo wget https://github.com/dogukannulu/send_data_to_aws_services/raw/main/json_to_kinesis_streams/json_to_kinesis_streams.zip
```

5. After modifications (if necessary), we can execute the shell script.
```
sudo ./setup.sh
```

6. We can monitor the live logs since it will print every single record.
```
sudo tail -f /project/json_to_kinesis_streams.log
```

## Notes

- All the processes will be running in `/project` directory after executing `setup.sh`
- You can see all the logs in `/project/json_to_kinesis_streams.log` 
- Shell script will download necessary packages, libraries first. Then, will install requirements and run the Python script
