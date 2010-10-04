__authors__   = "Guillaume Desjardin, Xavier Muller"
__copyright__ = "(c) 2010, Universite de Montreal"
__license__   = "3-clause BSD License"
__contact__   = "Xavier Muller <xav.muller@gmail.com>"


import numpy, os, shutil, sys, pickle
import pdb
from jobman.parse import filemerge
from jobman.parse import standard as jparse






def get_dir_by_key_name(dirs=['/data/lisa/exp/mullerx/exp/dae/mullerx_db/ms_0050'],key='nb_groups'):
    '''
    Returns a 3-tuple
    The first is the number of key values that where found in the directory
    The second is a list of all the key values found
    The third is a list of list of directories. It contains nb_key_value lists
    Each list contains the folder names 
    '''

    nb_key_values=0
    dir_list=[]
    nb_dir_per_group=[]
    key_values=[]

    for base_dir in dirs:
        for expdir in os.listdir(base_dir):
            

            confdir = os.path.join(base_dir, expdir)
            conf = os.path.join(confdir, 'current.conf')

            # No conf file here, go to next dir.
            if not os.path.isfile(conf):
                continue 

            keys_to_match = {}
            params = filemerge(conf)
            
 
            # Get the keyvalue in the conf file.
           
            kval = params.get(key, None)
            new_key=-1;
            # Check if we have this key value already.
            for i in range(len(key_values)):
                if kval==key_values[i]:
                    new_key=i
            

            # Update dir list accordingly.
            if new_key==-1:
                key_values.append(kval)
                nb_dir_per_group.append(1)
                dir_list.append([])
                dir_list[nb_key_values].append([confdir,expdir])
                nb_key_values= nb_key_values+1
            else:
                nb_dir_per_group[new_key]=nb_dir_per_group[new_key] + 1
                dir_list[new_key].append([confdir,expdir])


    if(nb_key_values==1):
        return (nb_key_values,key_values,dir_list)

    # Check if we have the same number of elements in each group.
    # This means some experiments have failed
    
    for i in range(nb_key_values):
        for j in range(nb_key_values):
            if (nb_dir_per_group[i]!=nb_dir_per_group[j]):
                raise EnvironmentError('Not all experiments where found, They might have crashed.... This is not supported yet')


    # Reparse the list based on first key.
    # Sort all the other lists so
    # The same conf parameters values show up always in the same order in each group 
    # Do it the slow lazy way as this code is not time critical
    for i in range(len(dir_list[0])):
        conf = os.path.join(dir_list[0][i][0], 'orig.conf')
        original_params=filemerge(conf)
        
        for j in range(1,nb_key_values):
            for k in range(nb_dir_per_group[0]):
                # Parse each group until we match the exact dictionnary (exept for or our key),
                # then swap it within the gorup so it has the same index as in group 0.
                conf = os.path.join(dir_list[j][k][0], 'orig.conf')
                current_params=filemerge(conf)
                current_params[key]=original_params[key]
                if current_params==original_params:
                    temp=dir_list[j].pop(k)
                    dir_list[j].insert(i,temp)
            
                    
                    
    return (nb_key_values,key_values,dir_list)

    

def get_dir_by_key_value(dirs=['/data/lisa/exp/mullerx/exp/dae/mullerx_db/ms_0043'],keys=['seed=0']):
    '''
    Returns a list containing the name of the folders. Each element in the list is a list
    containing the full path and the id of the experiment as a string
    '''
 
    good_dir = []

   
    # Gather results.
    for base_dir in dirs:
        for expdir in os.listdir(base_dir):

            skip = False

            confdir = os.path.join(base_dir, expdir)
            conf = os.path.join(confdir, 'current.conf')
            if not os.path.isfile(conf):
                continue

            params = filemerge(conf)


            skip = len(keys)

            for k in keys:
                keys_to_match = {}
                subkeys = k.split(':')
                for t in subkeys:
                    if t.find('=') != -1:
                        keys_to_match.update(jparse(t))
                       
                    else:
                        if len(subkeys) != 1:
                            raise ValueError('key1:key2 syntax requires keyval pairs')
                        kval = params.get(t)
                        skip -= 1

                for fkey,fval in keys_to_match.iteritems():
                    kval = params.get(fkey, None)
                    if kval == fval:
                        skip -= 1
                        break


            if not skip:
                good_dir.append([confdir,expdir])
                

                
            

    return good_dir

