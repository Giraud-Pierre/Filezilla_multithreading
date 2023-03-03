import logging
import os
import time
from Directory import Directory
from File import File
from talk_to_ftp import TalkToFTP
import threading
import queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


class DirectoryManagerMultithreading:
    def __init__(self, ftp_website, directory, depth, nb_threads, excluded_extensions):
        print("Multithreading engaged")

        self.root_directory = directory
        self.depth = depth
        # list of the extensions to exclude during synchronization
        self.excluded_extensions = excluded_extensions
        # max number of threads to use at the same time
        self.max_nb_threads = nb_threads
        # dictionary to remember the instance of File / Directory saved on the FTP
        self.synchronize_dict = {}
        self.os_separator_count = len(directory.split(os.path.sep))
        # list of the path explored for each synchronization
        self.paths_explored = []
        # list of the File / Directory to removed from the dictionary at the end
        # of the synchronization
        self.to_remove_from_dict = []
        # list of all file and directories to be removed
        self.path_removed_list = []
        # FTP instances
        self.ftp = TalkToFTP(ftp_website)
        self.ftp_multithreading = [TalkToFTP(ftp_website) for _ in range(nb_threads)]
        #Create Queue for multithreading
        self.queue_files = queue.Queue() #Queue of files to add or update
        self.queue_directories = queue.Queue() #Queue of directories to add
        self.queue_to_remove = queue.Queue() #Queue of directories or files to remove    
        #List to store the threads
        self.threads_updater = [threading.Thread(target = self.update_multithreading, 
                                                 args= (self.ftp_multithreading[id],)) 
                                                 for id in range(self.max_nb_threads)]
        self.threads_remover = [threading.Thread(target = self.remove_multithreading, 
                                                 args= (self.ftp_multithreading[id],)) 
                                                 for id in range(self.max_nb_threads)]
        #Launch the threads updaters to update directories and files each with a ftp connection     
        for id in range(self.max_nb_threads):
            self.threads_updater[id].start()
        #Launch the threads removers
        for id in range(self.max_nb_threads):
            self.threads_remover[id].start()
            

        # create the directory on the FTP if not already existing
        self.ftp.connect()
        if self.ftp.directory.count(os.path.sep) == 0:
            # want to create directory at the root of the server
            directory_split = ""
        else:
            directory_split = self.ftp.directory.rsplit(os.path.sep, 1)[0]
        if not self.ftp.if_exist(self.ftp.directory, self.ftp.get_folder_content(directory_split)):
            self.ftp.create_folder(self.ftp.directory)
        self.ftp.disconnect()

    def synchronize_directory(self, frequency):
        print("synchronize_directory multithreading")
        while True:
            # Store start time to evaluate the performance of the algorythm
            start=time.time()
            
            # init the path explored to an empty list before each synchronization
            self.paths_explored = []

            #init the path_removed_list to an empty list before each synchronization
            self.path_removed_list = []

            # init to an empty list for each synchronization
            self.to_remove_from_dict = []

            # Empty all queues in case some are not already (which should not happen
            # but, just for extra safety, let's empty them before each synchronization)
            self.queue_directories.empty()
            self.queue_files.empty()
            self.queue_to_remove.empty()

            #Start all the ftp connections
            self.ftp.connect()
            for id in range(self.max_nb_threads):
                self.ftp_multithreading[id].connect()

            # search for an eventual updates of files in the root directory
            print("start searching")
            self.search_updates(self.root_directory)

            # look for any removals of files / directories
            print("start removing")
            self.any_removals()
            print("end removing")

            #End all the ftp connections
            self.ftp.disconnect()
            for id in range(self.max_nb_threads):
                self.ftp_multithreading[id].disconnect()

            #Print time it took to perform the operations to evaluate performance
            print(time.time()-start)   
            # wait before next synchronization
            time.sleep(frequency)

    def search_updates(self, directory):
        # scan recursively all files & directories in the root directory
        for file_path, dirs, files in os.walk(directory): 
            for dir_name in dirs:
                #Join the file_path to the dir_name to get the full path of the directory
                directory_full_path = os.path.join(file_path, dir_name)
                self.queue_directories.put(directory_full_path)
 
            for file_name in files:
                #Join the file_path to the dir_name to get the full path of the directory
                file_full_path = os.path.join(file_path, file_name)
                self.queue_files.put([file_path,file_name,file_full_path])        

        while(self.queue_directories.qsize() > 0 and self.queue_files.qsize() > 0):
            time.sleep(1e-3)


    def update_multithreading(self, ftp):
        
        while(True):
            if(self.queue_directories.qsize() > 0):
                #Get the next path to update
                directory_path = self.queue_directories.get()

                # get depth of the current directory by the count of the os separator in a path
                # and compare it with the count of the root directory
                if self.is_superior_max_depth(directory_path) is False:
                    self.paths_explored.append(directory_path)

                    # a directory can't be updated, the only data we get is his creation time
                    # a directory get created during running time if not present in our list

                    if directory_path not in self.synchronize_dict.keys():
                        # directory created
                        # add it to dictionary
                        self.synchronize_dict[directory_path] = Directory(directory_path)

                        # create it on FTP server
                        split_path = directory_path.split(self.root_directory)
                        srv_full_path = '{}{}'.format(ftp.directory, split_path[1])
                        directory_split = srv_full_path.rsplit(os.path.sep,1)[0]
                        if not ftp.if_exist(srv_full_path, ftp.get_folder_content(directory_split)):
                            # add this directory to the FTP server
                            ftp.create_folder(srv_full_path)

            elif(self.queue_files.qsize() > 0):

                #Get the next file to update
                file_path, file_name, file_full_path = self.queue_files.get()

                # get depth of the current file by the count of the os separator in a path
                # and compare it with the count of the root directory
                if self.is_superior_max_depth(file_full_path) is False and \
                        (self.contain_excluded_extensions(file_full_path) is False):

                    self.paths_explored.append(file_full_path)
                    # try if already in the dictionary
                    if file_full_path in self.synchronize_dict.keys():

                        # if yes and he get updated, we update this file on the FTP server
                        if self.synchronize_dict[file_full_path].update_instance() == 1:
                            # file get updates
                            split_path = file_full_path.split(self.root_directory)
                            srv_full_path = '{}{}'.format(ftp.directory, split_path[1])
                            ftp.remove_file(srv_full_path)
                            # update this file on the FTP server
                            ftp.file_transfer(file_path, srv_full_path, file_name)

                    else:

                        # file get created
                        self.synchronize_dict[file_full_path] = File(file_full_path)
                        split_path = file_full_path.split(self.root_directory)
                        srv_full_path = '{}{}'.format(ftp.directory, split_path[1])
                        # add this file on the FTP server
                        ftp.file_transfer(file_path, srv_full_path, file_name)

            time.sleep(0)

    
    def any_removals(self):
        # if the length of the files & directories to synchronize == number of path explored
        # no file / directory got removed
        if len(self.synchronize_dict.keys()) == len(self.paths_explored):
            return
        
        # set the list and the Queue of the files & directories to be removed
        self.path_removed_list = []
        for path in self.synchronize_dict.keys():
            if(path not in self.paths_explored):
                self.path_removed_list.append(path)
                self.queue_to_remove .put(path)

        while(self.queue_to_remove.qsize() > 0):
            time.sleep(1e-3)

        # all the files / directories deleted in the local directory need to be deleted
        # from the dictionary use to synchronize
        for to_remove in self.to_remove_from_dict:
            if to_remove in self.synchronize_dict.keys():
                del self.synchronize_dict[to_remove]
    
    def remove_multithreading(self, ftp):
        while(True):
            if(self.queue_to_remove.qsize() > 0):             
                removed_path = self.queue_to_remove.get()

                # check if the current path is not in the list of path already deleted
                # Should not be very useful since we use a queue here but we kept it for safefty purposes
                if removed_path not in self.to_remove_from_dict:
                    # get the instance of the files / directories deleted
                    # then use the appropriate methods to remove it from the FTP server
                    if isinstance(self.synchronize_dict[removed_path], File):
                        split_path = removed_path.split(self.root_directory)
                        srv_full_path = '{}{}'.format(ftp.directory, split_path[1])
                        ftp.remove_file(srv_full_path)
                        self.to_remove_from_dict.append(removed_path)

                    elif isinstance(self.synchronize_dict[removed_path], Directory):
                        split_path = removed_path.split(self.root_directory)
                        srv_full_path = '{}{}'.format(ftp.directory, split_path[1])
                        self.to_remove_from_dict.append(removed_path)
                        # if it's a directory, we need to delete all the files and directories he contains
                        self.remove_all_in_directory(removed_path, srv_full_path, self.path_removed_list, ftp)

            time.sleep(0)

    def remove_all_in_directory(self, removed_directory, srv_full_path, path_removed_list, ftp):
        directory_containers = {}
        for path in path_removed_list:

            # path string contains removed_directory and this path did not get already deleted
            if removed_directory != path and removed_directory in path \
                    and path not in self.to_remove_from_dict:

                # if no path associated to the current depth we init it
                if len(path.split(os.path.sep)) not in directory_containers.keys():
                    directory_containers[len(path.split(os.path.sep))] = [path]
                else:
                    # if some paths are already associated to the current depth
                    # we only append the current path
                    directory_containers[len(path.split(os.path.sep))].append(path)

        # sort the path depending on the file depth
        sorted_containers = sorted(directory_containers.values())

        # we iterate starting from the innermost file
        for i in range(len(sorted_containers)-1, -1, -1):
            for to_delete in sorted_containers[i]:
                to_delete_ftp = "{0}{1}{2}".format(ftp.directory, os.path.sep, to_delete.split(self.root_directory)[1])
                if isinstance(self.synchronize_dict[to_delete], File):
                    ftp.remove_file(to_delete_ftp)
                    self.to_remove_from_dict.append(to_delete)
                else:
                    # if it's again a directory, we delete all his containers also
                    self.remove_all_in_directory(to_delete, to_delete_ftp, path_removed_list)
        # once all the containers of the directory got removed
        # we can delete the directory also
        ftp.remove_folder(srv_full_path)
        self.to_remove_from_dict.append(removed_directory)

    # subtract current number of os separator to the number of os separator for the root directory
    # if it's superior to the max depth, we do nothing
    def is_superior_max_depth(self, path):
        if (len(path.split(os.path.sep)) - self.os_separator_count) <= self.depth:
            return False
        else:
            return True

    # check if the file contains a prohibited extensions
    def contain_excluded_extensions(self, file):
        extension = file.split(".")[1]
        if ".{0}".format(extension) in self.excluded_extensions:
            return True
        else:
            return False
