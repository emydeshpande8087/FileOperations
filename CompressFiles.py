import gzip
import shutil
import sys,os
import tarfile
import zipfile
from UtilityFunctions import env_cpu_count, register_to_snowflake,upload_manager,get_list_of_files_from_s3,get_logger,register_to_dynamo,update_into_dynamo,download_manager,local_path
from concurrent.futures import ProcessPoolExecutor
# Get a logger for the current script
log = get_logger(__name__)


def StartCompression(local_file_name,compression_format):
    try:
        log.info("Starting Compression of :%s",local_file_name)
        if compression_format=="gzip":
            compressedOutFileName=local_file_name.split('.')[0]+".gzip"
            log.info("Compressed Output file name will be : %s",compressedOutFileName)
            with open(local_file_name, 'rb') as f_in, gzip.open(compressedOutFileName, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(local_file_name) 
        elif compression_format=="targz":
            compressedOutFileName=local_file_name.split('.')[0]+".targz"
            log.info("Compressed Output file name will be : %s",compressedOutFileName)
            with tarfile.open(compressedOutFileName, "w:gz") as tar_file:
                tar_file.add(local_file_name)
            os.remove(local_file_name)
        elif compression_format=="zip":
            compressedOutFileName=local_file_name.split('.')[0]+".zip"
            log.info("Compressed Output file name will be : %s",compressedOutFileName)
            with zipfile.ZipFile(compressedOutFileName, 'w',compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(local_file_name)
            os.remove(local_file_name)
        else:
            log.info("Invalid Compression format ! ")              
    except Exception as error:
        raise error

def CompressFiles(**kwargs):
    try:
        i_compression_format=kwargs['compression_format']
        p_task_id=kwargs['task_id']
        log.info("Compression Format is : %s",i_compression_format)
        file_q=os.listdir(local_path)
        log.info("List of files in processing queue: %s" , file_q)
        with ProcessPoolExecutor(max_workers=env_cpu_count) as ppe:
            futures = [ppe.submit(StartCompression, local_file_name=os.path.join(local_path,onefile),compression_format=i_compression_format) for onefile in file_q]
            for future in futures:
                future.result()  # Wait for each task to complete            
        log.info("Files ready for Upload are : %s", os.listdir(local_path))
    except Exception as e:
        error_message=str(e)
        log.error(f"Failed to Compress some files {error_message}")
        update_into_dynamo(task_id=p_task_id,processing_status='failed',desc_msg=error_message)
        raise

if __name__=='__main__':       
    if len(sys.argv)!=8:
        log.info('Incorrect arguments passsed ! , please send args in this sequence')
        log.info(f"Task id  , Customer id , job id , execution id , input folder , output folder , Compression Format")
    else:
        try:
            task_id=sys.argv[1]
            customerId=sys.argv[2]
            jobId=sys.argv[3]
            executionId=sys.argv[4]
            inputFolder=sys.argv[5]      
            outputFolder=sys.argv[6]
            compressionFormat=sys.argv[7]
            if compressionFormat not in ('gzip','zip','targz'):
                log.error("Incorrect format specified for compression.")
            log.info("Recieved arguments for Compress request as follows")
            log.info(f"Task id : {task_id} , Customer id : {customerId}, job id : {jobId}, execution id : {executionId}, input folder : {inputFolder}, output folder : {outputFolder}, Compression Format : {compressionFormat}")
            #create registry 
            register_to_dynamo(customer_name=customerId,execution_id=executionId,
                           job_id=jobId,operation_type='compression',task_id=task_id)
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'compression', processing_status = 'inprogress')
            #get list of files from s3
            file_queue=get_list_of_files_from_s3(p_input_folder=inputFolder,p_task_id=task_id)
            #download files to tmp folder 
            download_manager(p_file_queue=file_queue,p_task_id=task_id)
            #start compressing files
            CompressFiles(compression_format=compressionFormat,task_id=task_id) 
            #upload back to s3 
            upload_manager(p_task_id=task_id,p_output_folder=outputFolder)
            update_into_dynamo(task_id=task_id,processing_status='finished')
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'compression', processing_status = 'finished')
        except Exception as e:
            sys.exit(-1)        
