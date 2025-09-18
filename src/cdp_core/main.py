import sys
import importlib


def main():
    """
    Function to dynamically import a transformation module and executes its 'execute' function with two arguments. 
    
    Note, this is the entry point for the whl file. 

    The below arguments are passed as positional arguments within the Python Wheel Task in the specified Databricks Jobs.

    Arguments:
        sys.argv[1] (str): Name of the module to import. The target module must contain a function named 'execute' that accepts two arguments.
        sys.argv[2] (str): First argument to pass to the 'execute' function, this is the dataset.
        sys.argv[3] (str): Second argument to pass to the 'execute' function, this is the catalog     

    Example:
        Refer to the `bronze_task` in the `jb_lad_cd_nm_whl` example job in the `Jobs & Pipelines` panel
    """
    try:
        module = importlib.import_module(sys.argv[1])
        func = getattr(module, "execute")
        func(sys.argv[2], sys.argv[3])
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
