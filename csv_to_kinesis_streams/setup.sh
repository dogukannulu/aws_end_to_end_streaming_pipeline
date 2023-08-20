#!/bin/bash

log_file="/project/csv_to_kinesis_streams.log"

# Function to log messages to a log file
log_message() {
    local log_text="$1"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $log_text" >> "$log_file"
}

# Function to check if the script is being run with root privileges
check_root_privileges() {
    if [[ $EUID -ne 0 ]]; then
        log_message "This script is running with root privileges"
        exit 1
    fi
}

# Function to install packages using yum
install_packages() {
    local packages=(python3 python3-pip wget unzip)
    log_message "Installing required packages: ${packages[*]}"
    yum update -y
    yum install -y "${packages[@]}"
}

# Function to unzip the files
unzip_files() {
    log_message "Unzipping the files"
    unzip -o csv_to_kinesis_streams.zip
}

# Function to install required Python libraries
install_python_libraries() {
    local requirements_file="requirements.txt"
    log_message "Installing Python libraries from $requirements_file"
    pip3 install -r "$requirements_file"
}

# Function to execute the Python script
execute_python_script() {
    local csv_to_kinesis_streams_script="csv_to_kinesis_streams.py"
    local stream_name="csv-to-kinesis-streams-dogukan-ulu"
    local interval=5
    local max_rows=100
    local csv_url="https://raw.githubusercontent.com/dogukannulu/send_data_to_aws_services/main/csv_to_kinesis_streams/dirty_store_transactions.csv"
    
    log_message "Executing the Python script"
    chmod +x "$csv_to_kinesis_streams_script"
    python3 "$csv_to_kinesis_streams_script" --stream_name "$stream_name" \
        --interval "$interval" \
        --csv_url "$csv_url" \
        --max_rows "$max_rows"
}

# Main function to run the entire script
main() {
    log_message "Starting the script"
    check_root_privileges
    install_packages
    download_zip_file
    unzip_files
    install_python_libraries
    execute_python_script
    log_message "Script execution completed"
}

# Run the main function and redirect stdout and stderr to the log file
main >> "$log_file" 2>&1
