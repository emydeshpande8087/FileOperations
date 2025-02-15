from datetime import datetime
import time
import shutil
import sys,os

from UtilityFunctions import env_cpu_count, register_to_snowflake,upload_manager,get_list_of_files_from_s3,get_logger,register_to_dynamo,update_into_dynamo,download_manager,local_path
from concurrent.futures import ProcessPoolExecutor,wait
# Get a logger for the current script
log = get_logger(__name__)


def StartMerge(slice_to_process,out_file_name):
    try:
        log.info("Processing slice of files %s",slice_to_process)
        if len(slice_to_process)==1:
            os.rename(os.path.join(local_path,slice_to_process[0]),os.path.join(local_path,out_file_name))
            
        else:
        
            with open(os.path.join(local_path,out_file_name), 'ab') as fout:
                for i in slice_to_process:
                    with open(os.path.join(local_path,i), 'rb') as fin:
                        shutil.copyfileobj(fin, fout)
            log.debug("Removing slices and leftovers.....")
            for i in slice_to_process:
                log.info("Removing tmp file %s",i)
                os.remove(os.path.join(local_path,i))

    except Exception as error:
        raise error

def MergeFiles(**kwargs):
    try:
        i_file_format=kwargs['file_format']
        p_task_id=kwargs['task_id']
        customerId=kwargs['customerId']
        timestamp=str(datetime.now().strftime('%H:%M:%S'))
        log.info("File Format is : %s",i_file_format)
        file_q=os.listdir(local_path)
        outputName=f'merged_{customerId}_'+timestamp+'.'+i_file_format        
        log.info("List of files in processing queue: %s" , file_q)
        indexno=len(file_q)//2
        slice_1=file_q[:indexno]
        slice_2=file_q[indexno:]
        log.info("Slice 1 of process queue is : %s",slice_1)
        log.info("Slice 2 of process queue is : %s",slice_2)
        with ProcessPoolExecutor(max_workers=2) as ppe:
            future_1 = ppe.submit(StartMerge, slice_to_process=slice_1,out_file_name='merged_slice_1.tmp')
            future_2 = ppe.submit(StartMerge, slice_to_process=slice_2,out_file_name='merged_slice_2.tmp')
            wait([future_1, future_2])
            log.info("Files after first iteration in local %s",os.listdir(local_path))
            final_slice=['merged_slice_1.tmp','merged_slice_2.tmp']            
            future_3 = ppe.submit(StartMerge, slice_to_process=final_slice,out_file_name=outputName)
            result3=future_3.result()           
        log.info("Files ready for Upload are : %s", os.listdir(local_path))
    except Exception as e:
        error_message = str(e)
        log.error(f"Failed to Merge some files {error_message} ")
        update_into_dynamo(task_id=p_task_id,processing_status='failed',desc_msg=error_message)
        raise

if __name__=='__main__':       
    if len(sys.argv)!=8:
        log.info('Incorrect arguments passsed ! , please send args in this sequence')
        log.info(f"Task id  , Customer id , job id , execution id , input folder , output folder , File Format")
    else:
        try:
            task_id=sys.argv[1]
            customerId=sys.argv[2]
            jobId=sys.argv[3]
            executionId=sys.argv[4]
            inputFolder=sys.argv[5]      
            outputFolder=sys.argv[6]
            fileFormatToScan=sys.argv[7]
            if fileFormatToScan not in ('txt','csv'):
                log.error("Incorrect format specified for Merging.")
            log.info("Recieved arguments for Merge request as follows")
            log.info(f"Task id : {task_id} , Customer id : {customerId}, job id : {jobId}, execution id : {executionId}, input folder : {inputFolder}, output folder : {outputFolder}, File Format : {fileFormatToScan}")
            start_time=time.time() #record start time for analysis
            #create registry 
            register_to_dynamo(customer_name=customerId,execution_id=executionId,
                           job_id=jobId,operation_type='merge',task_id=task_id)
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'merge', processing_status = 'inprogress')
            #get list of files from s3
            unfiltered_file_queue=get_list_of_files_from_s3(p_input_folder=inputFolder,p_task_id=task_id)
            file_queue=[i for i in unfiltered_file_queue if i.endswith(fileFormatToScan)]
            #download files to tmp folder 
            download_manager(p_file_queue=file_queue,p_task_id=task_id)
            #start compressing files
            MergeFiles(file_format=fileFormatToScan,task_id=task_id,customerId=customerId) 
            #upload back to s3 
            upload_manager(p_task_id=task_id,p_output_folder=outputFolder)
            update_into_dynamo(task_id=task_id,processing_status='finished')
            register_to_snowflake(id = executionId, job_id = jobId, transformation_type = 'merge', processing_status = 'finished')
            end_time = time.time()
            # Calculate elapsed time
            elapsed_time_seconds = end_time - start_time
            elapsed_time_minutes = elapsed_time_seconds // 60
            remaining_seconds = elapsed_time_seconds % 60
            log.info(f"Operation took {elapsed_time_minutes:.0f} minutes and {remaining_seconds:.2f} seconds.")
        except Exception as e:
            log.error("Error Occured !")
            sys.exit(-1)        
