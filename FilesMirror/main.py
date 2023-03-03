import sys
import os
from directory_manager import DirectoryManager
from directory_manager_multithreading import DirectoryManagerMultithreading
from get_parameters import get_user_parameters

if len(sys.argv) == 1:
    current_dir = os.path.realpath(os.path.dirname(__file__))
    local_dir = current_dir[:-11] + "FTPServerLocal"
    sys.argv.extend(("localhost,test,test,/test,21", local_dir, "2", "30", "1", "5", ))

if __name__ == "__main__":
    # get parameters from command line
    ftp_website, local_directory, max_depth, refresh_frequency, is_multithreading, nb_threads, excluded_extensions = get_user_parameters()

    # init the correct directory manager with local directory and maximal depth using multithreading if it is asked
    if(is_multithreading):
        directory_manager = DirectoryManagerMultithreading(ftp_website, local_directory, max_depth, nb_threads, excluded_extensions)
    else:
        directory_manager = DirectoryManager(ftp_website, local_directory, max_depth, excluded_extensions)

    # launch the synchronization
    directory_manager.synchronize_directory(refresh_frequency)
