import subprocess
import os
import re
import yaml
import shutil
import  logging
import sys
import os.path
from os import path
import boto3
import botocore
from string import Template
from docx import Document

# logger = logging.getLogger(__name__)

def configure_dual_logging(verbosity, log = None, file = None):
    '''
    Configures logging with verbosity and log to file options. Default is logging to console only.
    Inputs:
    verbosity (string) - set the verbosity of the logging (v = errors and warnings, vv = errors, warnings, info,
        vvv = errors, warnings, info, and debug)
    log (string) - option to log to file
        -log new = start a new log file
        -log app = append to an existing log file
        -log = start a new log file
    file (string) - name of the log file
    '''
    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "LM_ImageProc_ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "LM_ImageProc_WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "LM_ImageProc_INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "LM_ImageProc_DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "LM_ImageProc_ERROR")
        logging.addLevelName(logging.WARNING, "LM_ImageProc_WARNING")
        logging.addLevelName(logging.INFO, "LM_ImageProc_INFO")
        logging.addLevelName(logging.DEBUG, "LM_ImageProc_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]


    logging.basicConfig(level=level, format=FORMAT, stream=sys.stderr)


    if log == 'new':
        open(file, 'w').close()

    # create file handler
    if log is not None:
        fh = logging.FileHandler(file)
        fh.setLevel(level)

        logging.getLogger().addHandler(fh)

def configure_logging(verbosity):
    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)


    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "LM_ImageProc_ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "LM_ImageProc_WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "LM_ImageProc_INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "LM_ImageProc_DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "LM_ImageProc_ERROR")
        logging.addLevelName(logging.WARNING, "LM_ImageProc_WARNING")
        logging.addLevelName(logging.INFO, "LM_ImageProc_INFO")
        logging.addLevelName(logging.DEBUG, "LM_ImageProc_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]
    logging.getLogger().setLevel(level)


def run_shell_cmd(cmd, exit = True, silent = False): # Use subprocess.run for Python3.6 an subprocess.Popen for ?
    '''
    Run the shell command passed in to the function and handle errors.
    Input:
        file (cmd) - command to run
    Output:
        Result of the shell command (string)
    '''

    # # proc = subprocess.Popen(cmd,
    # #                        stdout=subprocess.PIPE,
    # #                        stderr=subprocess.PIPE,
    # #                        encoding='UTF-8',
    # #                        shell=True)
    #
    # proc = subprocess.Popen(cmd,
    #                         stdout=subprocess.PIPE,
    #                         stderr=subprocess.PIPE,
    #                         shell=True)
    #
    # out, err = proc.communicate()
    #
    # # Check to see if there was an error by checking the length of stderr stream
    # if len(err) != 0:
    #     # Print an error message consisting of the command that was run and resulting error
    #     logging.error("Error running '{0}':\n{1}".format(cmd, str(err)))
    #     exit(0)
    # return str(out)
    # res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, shell=True, encoding='UTF-8')
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, shell=True, encoding='UTF-8')

    except subprocess.CalledProcessError as e:
        if silent == False:
            logging.error("run_shell_cmd: Process error running '{0}':\n{1}".format(cmd, e))
        if exit is True:
            exit(0)
        else:
            return -1

    # Check to see if there was an error by checking the length of stderr stream
    if len(res.stderr) != 0:
        if silent == False:
            logging.error("run_shell_cmd: Error running '{0}':\n{1}".format(cmd, str(res.stderr)))
        if exit is True:
            exit(0)
        else:
            return -1
    output = re.sub('\n|^\s*', '', res.stdout)  # remove new line characters and spaces at the beginning of the output

    return str(output)


def get_dir_size(dir_path):
    '''
        Gets the total size of a directory.
        Input:
            dir_path (string) - path to the directory
        Output:
            total size of directory (int)
    '''
    total_size = 0

    try:
        for dirpath, dirnames, filenames in os.walk(dir_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)

        return total_size

    except FileNotFoundError as e:
        logging.error('Error in get_dir_size: {0} not found. Error {1}'.format(start_path, e))
        exit (0)
        # return 0

def get_s3_dir_size(bucket, prefix, client):
    '''
        Gets the total size of a directory on s3.
        Input:
            bucket (string) - s3 bucket where the directory is
        Output:
            total size of directory (int)
    '''
    total_size = 0
    objects = client.list_objects_v2(Bucket=bucket, Prefix = prefix)
    if objects['KeyCount'] > 0:
        for key in objects['Contents']:
            total_size += key['Size']
        return total_size

    elif objects['KeyCount'] == 0:
        return 0

def columns_to_dict(file, key_columns, value_columns, subs = {}, required_term = None, column_type = 'index', case = 'lower'):
    '''
    Takes a tab delimited file and returns a dictionary. The keys are the values in the columns
    listed in the key_columns list separated by pipes. The values are the values in the columns
    listed in the value_columns list separated by pipes. The function can be passed a dictionary of
    regex subsitutions to make to correct for formatting or nomenclature inconsistencies. The function
    can also be passed a required term which needs to be in the line which the key and value information
    is pulled from. For example, it may be required to have 'LMPN' if it's a nucleotide probe and this
    will ignore any rows that are antibody (LMPA) or staining (LMPS) probes.
    Input:
        file (string) - tab delimited file to be searched
        key_columns (list) - list of columns to be included in the dictionary key, separated by pipes
        value_columns (list) - list of columns to be included in the dictionary value, separated by pipes
        subs (dict, optional) - dictionary of regex substitutions. The key is searched for in the line text and
            replaced with the value if it is present. Default is an empty dictionary.
        required_term (string, optional) - terms that needs to be present in the line. Default is None
        column_names (string, optional) - Set to name if column names are used instead of column indexes
            for key_columms and value_columns. Default is 'index'
    Output:
        Dictionary with information from the key_columns (separated by pipes) as the keys information from the
        values_columns (separated by pipes) as the values.
    '''
    my_dict = {}
    with open(file) as f:
        first_line = f.readline()
        column_names = first_line.split('\t')
        if column_type == 'name':
            key_columns = [column_names.index(x) for x in key_columns]
            value_columns = [column_names.index(x) for x in value_columns]


        for line in f:
            if case == 'lower' and required_term is not None:
                line = line.lower()
                required_term = required_term.lower()
            if case == 'upper' and required_term is not None:
                line = line.lower()
                required_term = required_term.upper()

            for sub in subs:
                line = re.sub(sub, subs[sub], line)
            if required_term is not None and required_term not in line:
                continue
            line = line.strip().split("\t")


            key_list = []
            value_list = []
            for col in key_columns:
                try:
                    key_list += [line[col]]
                except IndexError as e:
                    logging.error('Error in columns_to_dict: List index out of range for value column. Error {0}'.format(e))
                    logging.error('Error in columns_to_dict: index is {0}, Line is: {1}'.format(col, line))
            key_string = '|'.join(key_list)

            for col in value_columns:
                try:
                    value_list += [line[col]]
                except IndexError as e:
                    logging.error('Error in columns_to_dict: List index out of range for value column. Error {0}'.format(e))
                    logging.error('Error in columns_to_dict: index is {0}, Line is: {1}'.format(col, line))
            value_string = '|'.join(value_list)


            if key_string not in my_dict:
                my_dict[key_string] = value_string

    f.close()
    return my_dict

def column_to_list(file, column):
    '''
    Takes a tab delimited file and returns a list of the values in a specified column
    Input:
        file (string) - tab delimited file to be searched
        column (int) - index of the column to be made into a list
    Output:
        List of the values in the specified column
    '''
    my_list = []
    with open(file) as f:
        for line in f:
            line = line.strip().split("\t")
            ID = line[column]

            if ID not in my_list:
                my_list += [ID]
    f.close()
    return my_list

def make_index_dict(value_list):
    '''
    Takes a row of column names and creates a dictionary where the keys are the column names and the
    values are the indexes of the column
    Input: row (tab delimited string)
    Output: Dictionary with column names and corresponding indexes
    '''
    my_dict = {}
    for col in value_list:
        my_dict[col] = value_list.index(col)
    return my_dict

def getColValue(file, search_term, column):
    '''
        Searches a file for a row matching the search_term and returns the value of a specified column
        in that row
        Input:
            term (string) - term being searched for
            file (string) - tab delimited file to be searched
            column (int) - index of the column to be returned
        Output:
            Value at the specified location in the file
        '''
    val = ""
    with open(file, 'r') as f:
        for line in f:
            if search_term in line:
                line = line.strip().split("\t")
                try:
                    val = line[column]
                except Exception as e:
                    logging.error('Error in getColValue for {0}: {1}'.format(file, e))
    f.close()
    return val

def getColValues(file, search_terms, output_columns, match = all):
    '''
        Searches a file for a row with columns matching the search_terms and returns the value of specified columns
        in that row.
        Input:
            search_terms (list) - terms being searched for
            file (string) - tab delimited file to be searched
            output_columns (list) - names of the columns to be returned
            match (all or any) - parameter indicating whether all or any of the search terms have to match
                for the row to match
        Output:
            Values at the specified location in the file
        '''
    vals = []
    with open(file) as f:
        first_line = f.readline()
        column_names = first_line.strip().split('\t')
        for line in f:
            if match(search_term.lower() in line.lower().strip().split('\t') for search_term in search_terms):
                line = line.strip().split("\t")
                for col in output_columns:
                    try:
                        val = line[column_names.index(col)]
                        if val not in vals:
                            vals += [val]
                    except Exception as e:
                        logging.error('Error in getColValues for {0}: {1}'.format(file, e))

    f.close()
    return vals


def getFromFile(file, term, regex_template):
    '''
    Searches for the input term in the ontology file and returns the corresponding LungMap ID.
    Input: term (string)
    Output: Lungmap ID and term - separated by a semicolon (string)
    '''

    regex = regex_template.substitute({'term': term})
    f = open(file, 'r')
    s = f.read()
    match = re.findall(regex, s, re.MULTILINE)
    if match != []:
        logging.debug('matched: ' + term)
        return match[0] + ";" + term
    f.close()
    logging.debug('getFromFile: {0} is not in {1}'.format(term, file))
    return term

def para2text(doc):
    '''
    Takes a Document object and returns the full text as a string by joining paragraphs together.
    Need to import docx to use this function
    Input:
    doc: docx Document object to be converted into a string
    Output: Full text of the document as a string
    '''
    rs = doc._element.xpath('.//w:t')
    return u" ".join([r.text for r in rs])

def my_grep (regex, document, group):
    '''
    Searches a document or string for a regex match similar to using grep. Returns an empty string
    if there is no match found.
    Input
    regex: regex statement to search for (string)
    document: document or string to be searched (string)
    group: regex capturing group to return (int)
    Output
    result: result of re.findall search using the regex statement (string)
    '''
    try:
        result = re.findall(regex, document)[0][group]
    except:
        result = ""

    result = re.sub('^\s*|\s*$','', result)  # remove any spaces at the beginning or end of the result string
    return result