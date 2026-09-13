import csv
import sys
import logging
import os
import json
from urllib.parse import urlparse
import boto3
s3 = boto3.client('s3')
from generic_scripts.utils.s3_utils import parse_s3_path, read_s3_path_data, write_s3_path, file_exists, delete_s3_path_data


# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def read_prm_file(source_system):
    """
    Read parameter file from CSV and return list of validated rows.
    
    Args:
        source_system: Source system name (e.g., 'cv1')
        
    Returns:
        List of validated parameter rows (active only)
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prm_file_path = os.path.join(project_root, f"parfiles/{source_system}", f"{source_system}_prm.csv")
    
    logger.info(f"Reading parameter file from: {prm_file_path}")
    params = []
    
    with open(prm_file_path, "r", encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if validate_row(row):
                if row["active"] == "Y":
                    params.append(row)
            else:
                logger.debug("Required fields are not available, skipping row in the prm data.")
    
    logger.info(f"Successfully read {len(params)} active parameters from {source_system}_prm.csv")
    return params


def write_prm_file(source_system, params):
    """
    Write parameter data to JSON files for each workflow.
    
    Args:
        source_system: Source system name (e.g., 'cv1')
        params: List of parameter rows to process
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_workflows = set()
   
    for row in params:
        wf_name = row["wf_name"]
        
        if wf_name in processed_workflows:
            logger.warning(f"Workflow '{wf_name}' already processed. Duplicate found, skipping row.")
            continue
        
        try:
            data = {
                "wf_name": row["wf_name"],
                "source_schema": row["source_schema"],
                "target_schema": row["target_schema"],
                "source_table": row["source_table"],
                "target_table": row["target_table"],
                "phase_name": row["phase_name"],
                "connection": row["conn"],
                "etl_batch_id": read_etl_batch_id(row["etl_batch_id_path"], source_system),
                "active": row["active"]
            }
            
            param_file_path = os.path.join(project_root, f"parfiles/{source_system}", f'{wf_name}_prm.json')
            
            # Write data to local file
            with open(param_file_path, "w", encoding='utf-8') as param_file:
                json.dump(data, param_file, indent=4)
            
            bucket = "dataforge-lake"
            key = f"parfiles/{source_system}/{wf_name}_prm.json"
            write_s3_path(bucket, key, param_file_path)
            logger.info(f"Parameter file successfully written to S3 bucket '{bucket}' at key '{key}'")
            
            # Clean up local file
            os.remove(param_file_path)
            logger.info(f"Local parameter file removed: {param_file_path}")                
                
            
            logger.info(f"Successfully wrote parameter file: {param_file_path}")
            processed_workflows.add(wf_name)
            
        except IOError as io_err:
            logger.error(f"IO error writing parameter file for workflow '{wf_name}': {io_err}")
            raise
        except Exception as err:
            logger.error(f"Error processing workflow '{wf_name}': {err}")
            raise                     

def read_etl_batch_id(etl_batch_id_path, source_system):
    try:
        # Read etl_batch_id from s3 using boto3
        bucket_name, file_key = parse_s3_path(etl_batch_id_path)
        logger.info("Successfully parsed s3 path for etl_batch_id")
        etl_batch_id = read_s3_path_data(bucket_name, file_key)
        logger.info(f"Sucesfully read etl_batch_id from bucket {bucket_name} and key {file_key}")
    except Exception as E:
        logger.warning(f"failed to get etl_batch_id from s3, reverting back to efs path: {E}")
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    etl_batch_id_path = os.path.join(project_root, f"parfiles/", f"{source_system}_batch_id.txt")
    with open(etl_batch_id_path, "r", encoding= 'utf-8') as f:
        etl_batch_id = f.readline().strip()
    return etl_batch_id

def validate_row(row):
    required_fields = [
        "etl_batch_id_path",
        "wf_name",
        "source_schema",
        "target_schema",
        "source_table",
        "target_table",
        "phase_name",
        "conn",
        "active"
    ]
    for field in required_fields:
        value = row.get(field)
        if value is None or not value.strip():
            logger.warning(
                f"Missing or empty required field '{field}' "
                f"for workflow '{row.get('wf_name', '<unknown>')}'. "
                "Skipping row."
            )
            return False
    if row["active"].strip().upper() not in ("Y", "N"):
        logger.warning(
            f"Invalid active value '{row['active']}' "
            f"for workflow '{row.get('wf_name', '<unknown>')}'. "
            "Expected 'Y' or 'N'. Skipping row."
        )
        return False
    return True


def main():
    """Main entry point - orchestrates reading and writing parameter files."""
    # Check for required arguments
    if len(sys.argv) < 2:
        logger.error("Missing required argument: source_system. Usage: python param_gen.py <source_system>")
        sys.exit(1)
    
    source_system = sys.argv[1]
    logger.info(f"Starting parameter generation for source system: {source_system}")
    
    try:
        # Read parameter file
        params = read_prm_file(source_system)
        
        if not params:
            logger.warning(f"No active parameters found for source system '{source_system}'")
            return
        
        # Write parameter files
        write_prm_file(source_system, params)
        logger.info(f"Parameter generation completed successfully for {source_system}")
        
    except FileNotFoundError as fnf_err:
        logger.error(f"Parameter file not found for source system '{source_system}': {fnf_err}")
        sys.exit(2)
    except IOError as io_err:
        logger.error(f"IO error while processing '{source_system}': {io_err}")
        sys.exit(3)
    except Exception as err:
        logger.error(f"Unexpected error while processing '{source_system}': {err}", exc_info=True)
        sys.exit(4)    
    
    
if __name__ == "__main__":
    main()       