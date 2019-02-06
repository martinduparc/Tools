#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 16:55:47 2018

@author: mmandal
"""

import argparse
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
import json
from string import Template
from utils import configure_logging, run_shell_cmd, get_dir_size, get_s3_dir_size, columns_to_dict, \
    column_to_list, make_index_dict, getColValue, getFromFile, configure_dual_logging, getColValues, my_grep
from create_pre_metadata_carson_nanodesi import create_pre_md_file
from create_pre_metadata_carson_nanodesi_with_xref import create_pre_md_file_with_xref


##################################################################
# Parse command line arguments

'''Create a parser and subparsers'''
parser = argparse.ArgumentParser(description="Create metadata files, create Ids, process images, and/or update tsv files")

parser.add_argument("-v",
                        action='count',
                        dest='verbosity_level',
                        required=False,
                        default=0,
                        help="Increase verbosity of the program."
                        "Multiple -v's increase the verbosity level:\n"
                        "0 = Errors\n"
                        "1 = Errors + Warnings\n"
                        "2 = Errors + Warnings + Info\n"
                        "3 = Errors + Warnings + Info + Debug")

parser.add_argument('-cf', '--config_file',
                           required=True,
                           action='store',
                           dest='cf')


# Arguments for each step involved in image processing and update metadata

parser.add_argument('-pre_metadata', '--create prelimiary metadata file from file names and protocols',
                            required=False,
                            action='store_true',
                            dest='pre_metadata')


parser.add_argument('-LM_metadata', '--create LM metadata file',
                            required=False,
                            action='store_true',
                            dest='LM_metadata')

parser.add_argument('-ids', '--create LungMap ids',
                            required=False,
                            action='store_true',
                            dest='ids')

parser.add_argument('-process', '--process iamges',
                            required=False,
                            action='store_true',
                            dest='process')

parser.add_argument('-tsv', '--update tsv files',
                            required=False,
                            action='store_true',
                            dest='tsv')

parser.add_argument('-reset', '--reset tsv files',
                            required=False,
                            action='store_true',
                            dest='reset')

# Arguments for image processing sub-steps

parser.add_argument('-mv', '--move',
                            required=False,
                            action='store_true',
                            dest='move')

parser.add_argument('-r', '--rotate',
                            required=False,
                            action='store_true',
                            dest='rotate')

parser.add_argument('-hm', '--home',
                            required=False,
                            action='store_true',
                            dest='home')

parser.add_argument('-tb',
                            '--thumbnail',
                            required=False,
                            action='store_true',
                            dest='thumbnail')

parser.add_argument('-ti',
                            '--tiles',
                            required=False,
                            action='store_true',
                            dest='tiles')

parser.add_argument('-sc',
                            '--scaling',
                            required=False,
                            action='store_true',
                            dest='scaling')

parser.add_argument('-gz',
                            '--gzip',
                            required=False,
                            action='store_true',
                            dest='gzip')

parser.add_argument('-s3',
                            '--s3',
                            required=False,
                            action='store_true',
                            dest='s3')

parser.add_argument('-s3f',
                            '--s3f',
                            required=False,
                            action='store_true',
                            dest='s3f')

# Arguments to output to a log file and add on to existing LM metadata file

parser.add_argument('-cont',
                            '--Add on to existing LM metadata file instead of creating a new file',
                            required=False,
                            action='store_true',
                            dest='cont')

parser.add_argument('-log',
                            '--Add on to an existing log file or output to a new log file',
                            required=False,
                            action='store',
                            choices=['app', 'new', 'None'],
                            default=None,
                            nargs='?',
                            const=None,
                            dest='log')


args = parser.parse_args()

##################################################################
# Load configurations from config file

with open(args.cf, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
for e in cfg:
    if type(cfg[e]) == list:
        cfg[e] = ''.join(cfg[e])

##################################################################
# Intialize logging, set up boto3 client, initialize ontology files and probeColorDict
# configure_logging(args.verbosity_level)
configure_dual_logging(args.verbosity_level, log = args.log, file = cfg['logFile'])
# logger = configure_dual_logging(args.verbosity_level, log = args.log, file = cfg['logFile'])



client = boto3.client(
    's3',
    aws_access_key_id='AKIAID6OPOO53YJ6UQWA',
    aws_secret_access_key='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',
)

ontology_regex_template = Template(r'\;(LM[HM]A[\d]{10})\"\>[^\!]*\<(?:rdfs\:label|breath\_database\:synonym|'
                                   r'breath\_database\:HTC\_Code)\>$term\<')
controlled_vocab_regex_template = Template(r"(LMCV[\d]{10})[^\n]*$term")

with open(cfg['probeColorsJson'], 'r') as probe_colors:
    probeColorDict = json.load(probe_colors)


##################################################################
# Function definitions

def matchProbeId(probeIdsDict, probeId, probeInfo):
    '''
    Searches current probe IDs and returns a probe ID that matches both in name and product number
    Input:
        probeIdsDict (dict) - dictionary of currently existing probe IDs including product number
        probeID (string) - the probe name of the probe being being matched
        probeInfo (string) - the probe information for the probe being amtched
    Output: Lungmap Probe ID
    '''
    for probeWithInfo in probeIdsDict.keys():

        probe, info = probeWithInfo.split("|")

        if probe.lower() == "nkx2-1":
            probe = "nkx2.1"
        if probeId.lower() == probe.lower() and info in probeInfo:
            return probeIdsDict[probeWithInfo]
    return ""

######################
# Extract information from protocols and create metadata file
def create_pre_metadata():
    logging.info("Creating preliminary metadata file...")

    # create_pre_md_file (cfg['metadataFile'], cfg['POS_metabolite_file'], cfg['NEG_metabolite_file'],
    #               cfg['processingDir'], cfg['biologicalDatabaseEntityTSV'])
    create_pre_md_file_with_xref(cfg['metadataFile'], cfg['POS_metabolite_file'], cfg['NEG_metabolite_file'],
                       cfg['processingDir'], cfg['biologicalDatabaseEntityTSV'], cfg['xrefFile'])

#####################
# Create list of file names
def create_filnames():
    filenameList = []
    md = open(cfg['metadataFile'], 'r')
    for line in md:
        line = line.split('\t')
        if len(line) > 1 and line[1] != 'FILE_NAME' and line[int(cfg['filename_column'])] != 'filename' and line[int(cfg['filename_column'])] != '':
            filenameList += [line[int(cfg['filename_column'])]]
    return filenameList

#####################
# Create LungMap Metadata file
def create_LM_metadata(filenameList):
    logging.info("Creating LungMap metadata file...")
    count = 1  # For incrementing fields when necessary
    added = 0 # for keeping track of the number of rows added

    '''Create the first line of the LungMap metadata file'''
    # Open LM metadata and tsvOrigins Files
    if args.cont == True:
        LM_metadata = open(cfg['processingDir'] + 'LM_metadata_file.txt', 'a+')
    else:
        LM_metadata = open(cfg['processingDir'] + 'LM_metadata_file.txt', 'w+')

    tsvOrigins = open(cfg['originsFile'], 'r')
    missingValues = open(cfg['missingValues'], 'w+')
    first_row = []

    # Loop through the tsvOrigins file and extract the first field (column name)
    for line in tsvOrigins:
        line = line.split('\t')
        column = line[0]

        # Skip the rows without data
        if column == "TSV Column" or column == "" or column == "Alltsv columns" \
                or column == "All tsv columns" or column[-3:] == 'tsv':
            continue
        first_row += [line[0]]
    first_row = "\t".join(first_row) + '\n'
    LM_metadata.write(first_row)
    tsvOrigins.close()

    '''Loop through file names and add rows to the Lungmap Metadata file'''
    for file in filenameList:

        # Grep for file name in the preliminarymetadata file
        file = re.sub('[\\\\|\s]', '', file)
        grep_cmd = "grep '{0}' {1}".format(file, cfg['metadataFile'])

        row = run_shell_cmd(grep_cmd)
        logging.debug("Filename: {0}".format(file))

        ### If the row is not empty and the file name is not already in the LungMap metadata file
        ### if row is not None and file not in open(cfg['metadataFileLM']).read():
        if 'ISH_metadata_Rutledge.txt' in cfg['metadataFile']:
            row = ' \t' + row
        if row is not None and file not in LM_metadata.read():
            LM_metadata_row = []  # Information that will go into the LM metadata row
            tsvOrigins = open(cfg['originsFile'], 'r')

            # Go through the tsvOrigins file and pull the metadata from the stated source (filename, metadata, constant)
            for line in tsvOrigins:

                # Split the tsvOrigins row and assign variables
                line = ''.join([i if ord(i) < 128 else ' ' for i in line])  # remove non-ascii characters
                line = line.split('\t')[:8]

                var, value, origin, regex, md_col, script, arguments, required = line
                metadata_value = value

                # Set the filename
                if var == "Filename":
                    metadata_value = file

                # Skip the rows without data
                elif "TSV Column" in var or var == "" or var == "Alltsv columns" \
                        or var == "All tsv columns" or var[-3:] == 'tsv':
                    continue

                # if the column name ends in tsv this marks the beginning of a new file
                elif var[-3:] == "tsv":
                    metadata_value = ""

                # Create values with sequential numbers
                elif origin == "increment":
                    metadata_value = metadata_value + str(count)
                    count += 1

                # Extract the value from the metadata file
                elif md_col != "" and origin == "metadata":
                    metadata_value = row.split('\t')[int(md_col)]
                    # metadata_value = row.split('\t')[int(md_col)+1]
                # Extract the value from the filename
                elif origin == "filename" or origin == 'folder_name':
                    file = re.sub(', ', '_', file)
                    if re.search(regex, file) == None:
                        metadata_value = ''
                    else:
                        # metadata_value = re.search(regex, file)[1] # use this for non-nanodesi image processing
                        metadata_value = re.search(regex, file)[0]
                # Extract the value from a file
                # arguments need to be filename, searchterms, columns
                elif origin == "file":
                    arguments = arguments.split('|')
                    file_to_search = cfg[arguments[0]]
                    search_terms = arguments[1].split(',')
                    for i, search_term in enumerate(search_terms):
                        if search_term[:4] == 'fre-':
                            search_term = re.compile(search_term[4:])
                            if search_term.search(file) != None:
                                search_terms[i] = search_term.search(file)[0]
                            else:
                                search_terms = []
                    columns = arguments[2].split(',')
                    metadata_value = '|'.join(getColValues(file_to_search, search_terms, columns))
                    # metadata_value = re.search(regex, file)[1]

                # Run a script to get the value
                if script != "" and origin == 'metadata and script':
                    arguments = arguments.split('|')
                    argument_string = ''
                    for argument in arguments:
                        if argument[:3] == 'md-':
                            argument_string += " '" + row.split('\t')[int(argument[3:])] + "' "
                            # argument_string += "'" + row.split('\t')[int(argument[3:]) + 1] + "' "
                        elif argument[:4] == 'cfg-':
                            argument_string += " '" + cfg[argument[4:]] + "' "

                    script_path = "{0}{1}".format(cfg['scriptsDir'], script)

                    bash_cmd = "bash {0} {1}".format(script_path, argument_string)
                    entrez_id = run_shell_cmd(bash_cmd)

                    metadata_value = entrez_id

                metadata_value = re.sub('\n', '', metadata_value)  # remove end of line characters from metadata_value
                metadata_value = re.sub('^\|', '', metadata_value)  # remove beginning pipe from value

                if required == 'y' and (metadata_value == "" or metadata_value == 'blank') and origin != 'generated':
                    missingValues.write(file + ' is missing ' + var + '\n')
                LM_metadata_row += [metadata_value]  # append the information to the LM metadata row

            tsvOrigins.close()

        # Write the new row to the LM_metadata file

        LM_metadata_row = "\t".join(LM_metadata_row) + '\n'
        LM_metadata.write(LM_metadata_row)
        added += 1

    LM_metadata.close()
    missingValues.close()

    logging.info('Added {0} images to the LungMap metadata file'.format(added))

    # Check how many fields in the LungMap metadata file are missing values
    cmd = r'wc -l < ' + cfg['missingValues']
    numMissingValues = run_shell_cmd(cmd)
    logging.info('{0} fields in the LungMap metadata file are missing values'.format(numMissingValues))
    # print (numMissingValues + ' fields in the LungMap metadata file are missing values')

#####################
# Assign experiment IDs and create xref file
def assign_ids(filenameList):
    logging.info('Assigning LungMap IDs...')

    # Find last experiment ID
    tail_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMEX0*)(.*)/; print $2;'".format(cfg['expTSV'])
    expNumber = run_shell_cmd(tail_cmd)

    # Find last image ID
    tail_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMIM0*)(.*)/; print $2;'".format(cfg['imgTSV'])
    imgNumber = run_shell_cmd(tail_cmd)

    imgNumber = int(re.sub('\n', '', imgNumber))  # remove end of line characters from imgNumber
    expNumber = int(re.sub('\n', '', expNumber))  # remove end of line characters from expNumber
    logging.info('Last image number: ' + str(imgNumber))
    logging.info('Last exp number: ' + str(expNumber))

    lastExpPrefix = ""
    lastExpNum = 0
    expCounter = 0  # Keeps track of how many LungMap experiment IDs are generated
    imgCounter = 0  # Keeps track of how many LungMap image IDs are generated

    ## Go through the filenameList image by image. Create a new LungMap image ID for every file and the LungMap experiment
    ## ID for every unique expPrefix. For each image add a row to the xref file consisting of the file name, LungMap image
    ## ID, and LungMap experiment ID.
    open(cfg['xrefFile'], "w").close()
    for file in filenameList:
        if file == "Filename" or file == "FILE_NAME":
            continue

        # If the expPrefix is different from the expPrefix of the last image, generate a new LungMap experiment ID
        # expPrefix = file.split('/')[0]
        expPrefix = re.search(cfg['experiment_prefix_regex'], file)[0]
        # expPrefix = file.search(cfg['experiment_prefix_regex'])[0]
        if expPrefix != lastExpPrefix:
            lastExpNum = expNumber
            expNumber += 1
            lastExpPrefix = expPrefix
            zeroCount = 10-len(str(expNumber))
            expId = 'LMEX' + (str(0) * zeroCount) + str(expNumber)
            expCounter += 1
        imgNumber += 1

        # Generate a new LungMap image ID
        zeroCountImg = 10 - len(str(imgNumber))
        imgId = 'LMIM' + (str(0) * zeroCountImg) + str(imgNumber)
        img_tsv_string = "data\t{0}\texpression_image".format(imgId)
        fileRegex = re.sub(' ', '\ ', file)
        imgCounter += 1

        # Add a new row to the xref file
        grep_cmd = 'pcregrep -q "{0}\t{1}\t{2}" {3} || echo "{0}\t{1}\t{2}" >> {3}'.format(fileRegex, imgId,
                                                                                           expId, cfg['xrefFile'])
        run_shell_cmd(grep_cmd)

    logging.info(str(expCounter) + ' LungMap Experiment IDs generated')
    logging.info(str(imgCounter) + ' LungMap Image IDs generated')

#####################
# Process images
def process_images():

    logging.info('Processing images...')

    xref = open(cfg['xrefFile'], 'r')
    if args.scaling == True:
        open(cfg['scalingFactors'], "w").close()  # create empty scaling factors file

    for line in xref:
        line = re.sub('\n', '', line)
        file_with_path, imgId, expId = line.split('\t')
        file = file_with_path.split('/')[-1]
        file_with_path = cfg['processingDir'] + file_with_path
        ofile = file  # original file name
        suffix = file.split('.')[-1]

        # Make image directory
        imgDir = cfg['processingDir'] + 'lungmap_breath_data/' + expId + '/' + imgId + '/'

        os.makedirs(imgDir, exist_ok=True)

        if args.move is True:
            if path.exists(imgDir + file):
                logging.info('File already in LungMap directory. Skipping move step for {0}'.format(file))
            else:
                try:
                    shutil.move(file_with_path, imgDir + file)
                    logging.info('Moved {0} to {1}'.format(file, imgDir))
                # except FileNotFoundError as e:
                #     logging.info('File does not exist. Skipping {0}: {1}'.format(file, e))
                except Exception as e:
                    logging.info('File does not exist. Skipping {0}: {1}'.format(file, e))

        if args.rotate is True:
            rfile = 'r-' + file
            if path.exists(rfile):
                logging.info('Rotated file already exists. Skipping rotation for {0}'.format(rfile))
            else:
                rotation_value_cmd = r'grep {0} {1} | perl -lne "@F = split("\t"); $F[25] ' \
                                     '=~ s/CCW90/270/; $F[25] =~ s/CW90/90/; $F[25] =~ s/C90/90/; print $F[25];"'.format(file, cfg['metadataFileLM'])
                rotation = run_shell_cmd(rotation_value_cmd, exit = False)
                rotate_cmd = 'convert -rotate {0} {1} {2}'.format(rotation, imgDir + file, imgDir + rfile)
                if int(rotation) > 0:
                    res = run_shell_cmd(rotate_cmd)
                    if res != -1:
                        logging.info('Rotated {0} {1} degrees'.format(file, rotation))
                        file = rfile
                    else:
                        logging.info('Could not rotate {0} {1} degrees'.format(file, rotation))

        if args.thumbnail is True:
            thumbnail_file_name = imgDir + imgId + '.' + 'thumb' + '.' + suffix
            if path.exists(thumbnail_file_name):
                logging.info('Thumbnail already exists. Skipping thumbnail for {0}'.format(file))
            else:
                thumb_cmd = 'convert -quiet -thumbnail x120 {0} {1}'.format(imgDir + file, thumbnail_file_name)
                res = run_shell_cmd(thumb_cmd, exit = False)
                if res != -1:
                    logging.info('Created thumbnail {0}'.format(thumbnail_file_name))
                else:
                    logging.info('Could not create thumbnail {0}'.format(thumbnail_file_name))

        if args.home is True:

            home_file_name = imgDir + imgId + '.' + 'home' + '.' + suffix
            if path.exists(home_file_name):
                logging.info('Home image already exists. Skipping home image for {0}'.format(file))
            else:
                home_cmd = 'convert {0} -resize "1000x338^" -gravity center -quiet -crop 1000x338+0+0 +repage ' \
                           '{1}'.format(imgDir + file, home_file_name)
                res = run_shell_cmd(home_cmd, exit = False)
                if res != -1:
                    logging.info('Created home image {0}'.format(home_file_name))
                else:
                    logging.info('Could not create home image {0}'.format(home_file_name))

        if args.tiles is True:

            tiles_folder = imgDir + 'tiles'

            if path.exists(tiles_folder):
                logging.info('Tiles already exist. Skipping tiles for {0}'.format(file))
            else:
                #  /usr/local/bin/gdal2tiles.py (path on lungmap image processing server)
                tiles_cmd = '{0}gdal2tiles.py -quiet -p raster {1} {2}'.format(cfg['scriptsDir'], imgDir + file,
                                                                               tiles_folder)
                # tiles_cmd = '{0}gdal2tiles.py -quiet -p raster {1} {2}'.format(cfg['toTilesDir'], imgDir + file, tiles_folder)
                res = run_shell_cmd(tiles_cmd, exit = False)
                if res != -1:
                    logging.info('Created tiles {0}'.format(tiles_folder))
                else:
                    logging.info('Could not create tiles {0}'.format(tiles_folder))

        if args.scaling is True:

            if path.exists(cfg['scalingFactors']) and file in open(cfg['scalingFactors']).read():
                logging.info('Scaling factors already calculated. Skipping scaling factors for {0}'.format(file))
            else:
                sf = open(cfg['scalingFactors'], "a+")
                scaling_cmd = "identify -quiet {0} | perl -lne '/(\d+)x(\d+)/; print $1'".format(imgDir + file)
                x = int(run_shell_cmd(scaling_cmd, exit = False, silent = True))

                scaling_cmd = "identify -quiet {0} | perl -lne '/(\d+)x(\d+)/; print $2'".format(imgDir + file)
                y = int(run_shell_cmd(scaling_cmd, exit = False, silent = True))

                if x == -1 and y == -1:
                    scaling_cmd = "identify -quiet {0} | perl -lne '/(\d+)x(\d+)/; print $1'".format(
                        imgDir + file + '.gz')
                    x = int(run_shell_cmd(scaling_cmd, exit=False, silent = True))

                    scaling_cmd = "identify -quiet {0} | perl -lne '/(\d+)x(\d+)/; print $2'".format(
                        imgDir + file + '.gz')
                    y = int(run_shell_cmd(scaling_cmd, exit=False, silent = True))


                if x != -1 and y != -1:
                    xTiles = 1
                    while x > (2 ** (xTiles) * 256):
                        xTiles += 1

                    yTiles = 1
                    while y > (2 ** (yTiles) * 256):
                        yTiles += 1

                    tileCount = max([xTiles, yTiles])
                    xScalingFactor = x / (2 ** tileCount * 256)
                    yScalingFactor = y / (2 ** tileCount * 256)
                    row = imgId + '\t' + str(xScalingFactor) + '\t' + str(yScalingFactor) + '\n'
                    sf.write(row)
                    logging.info('Calculated scaling factors for {0}'.format(file))
                else:
                    logging.info('Could not calculate scaling factors for {0}'.format(file))

        if args.gzip is True:
            zfile = ofile + '.gz'
            if path.exists(zfile):
                logging.info('Zip file already exists. Skipping gzip for {0}'.format(file))
            else:
                zip_cmd = 'gzip {0}'.format(imgDir + file)
                res = run_shell_cmd(zip_cmd, exit = False)
                if res != -1:
                    shutil.move(imgDir + file + '.gz', imgDir + zfile)
                    logging.info('Zipped File {0}'.format(zfile))
                else:
                    logging.info('Could not zip file {0}'.format(zfile))
        if args.s3 is True:

            file_path = '{0}/{1}/{2}/{3}'.format(cfg['processingDir'], 'lungmap_breath_data', expId, imgId)

            total_size = get_dir_size(file_path)
            prefix = '{0}/{1}/'.format(expId, imgId)

            bucket_name = 'lungmap-breath-data'

            total_size_s3 = get_s3_dir_size(bucket_name, prefix, client)

            if total_size == 0:
                logging.info('Uploading to s3 error: {0} is empty'.format(file_path))
            elif total_size == total_size_s3:
                logging.info('{0} is already uploaded to s3'.format(prefix))
            elif total_size_s3 != 0 and total_size > 0:
                logging.info('{0} exists on s3 but is a different size'.format(prefix))
                logging.info('local folder size is: {0}'.format(total_size))
                logging.info('S3 folder size is: {0}'.format(total_size_s3))
            else:
                s3_cmd = 'aws s3 sync {0}/lungmap_breath_data/{1}/{2} s3://lungmap-breath-data/{1}/{2} ' \
                                '--grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers'.format(cfg['processingDir'], expId, imgId)
                res = run_shell_cmd(s3_cmd, exit = False)
                if res != -1:
                    logging.info('Moved {0} to S3'.format(imgId))
                else:
                    logging.info('Could not move {0} to S3'.format(imgId))


        if args.s3f is True:
            s3f_cmd = 'aws s3 sync {0}/fasta_files s3://lungmap-supporting-files/ --recursive ' \
                        '--grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers'.format(cfg['processingDir'])
            run_shell_cmd(s3f_cmd, Exit = False)
            logging.info('Moved fasta files to S3')

#####################
# Update tsv files
def update_tsv():
    logging.info('Updating tsv files...')

    # Backup tsv files
    backup_tsv_cmd = '''
    cp {1} {0}sample_backup.tsv
    cp {2} {0}supporting_file_backup.tsv
    cp {3} {0}probe_backup.tsv
    cp {4} {0}probe_color_backup.tsv
    cp {5} {0}expression_image_backup.tsv
    cp {6} {0}experiment_backup.tsv
    cp {7} {0}analysis_entity_backup.tsv
    '''.format(cfg['tsvDir'], cfg['sampleTSV'], cfg['suppTSV'], cfg['probeTSV'], cfg['probeColorTSV'], cfg['imgTSV'],
               cfg['expTSV'], cfg['analysisEntityTSV'])
    run_shell_cmd(backup_tsv_cmd)

    # Make temporary probe file containing just the rows with the probe type of the experiment being uploaded
    if cfg['probeTypeLM'] != 'NA':
        temp_probe_cmd = 'pcregrep {0} {1} > "{2}"'.format(cfg['probeTypeLM'], cfg['probeTSV'], cfg['probeTSV'] + '.tmp')
        run_shell_cmd(temp_probe_cmd)
        probeTSV_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMP.0*)(.*)/; print $2;'".format(cfg['probeTSV'] + '.tmp')
        probeColorTSV_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMPC0*)(.*)/; print $2;'".format(cfg['probeColorTSV'])
        nextProbeNumber = int(run_shell_cmd(probeTSV_cmd)) + 1
        nextProbeColorNumber = int(run_shell_cmd(probeColorTSV_cmd)) + 1
    else:
        LMprobeColorIds = ''

    # Get next LungMap ID numbers - imgNumber and expNumber already found in previous step
    sampleTSV_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMSP0*)(.*)/; print $2;'".format(cfg['sampleTSV'])

    suppTSV_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMSF0*)(.*)/; print $2;'".format(cfg['suppTSV'])
    aeTSV_cmd = "tail -n 1 {0} | perl -lane '$F[1] =~ /(LMAE0*)(.*)/; print $2;'".format(cfg['analysisEntityTSV'])
    nextSampleNumber = int(run_shell_cmd(sampleTSV_cmd)) + 1
    nextSuppNumber = int(run_shell_cmd(suppTSV_cmd)) + 1
    nextAeNumber = int(run_shell_cmd(aeTSV_cmd)) + 1

    # Create dictionaries of the existing sample, probe, probe color and experiment IDs
    # existingSampleIds = columns_to_dict(cfg['sampleTSV'], [1], [3], required_term = 'LMSP')
    existingSampleIds = columns_to_dict(cfg['sampleTSV'], [3], [1], required_term='LMSP')
    # existingMZRanges = columns_to_dict(cfg['analysisEntityTSV'], ['mz_id'], ['start_mz', 'end_mz'],
    #                                    column_type = 'name', required_term='LMAE')
    existingMZRanges = columns_to_dict(cfg['analysisEntityTSV'], ['start_mz', 'end_mz'], ['mz_id'],
                                       column_type='name', required_term='LMAE', case = None)
    # newExpIds = getExistingIdsList(cfg['xrefFile'], 2)
    newExpIds = column_to_list(cfg['xrefFile'], 2)
    subs = {'nkx2-1': 'nkx2.1', 'vimentin': 'vim', "anti\-": ""}
    existingProbeIds = columns_to_dict(cfg['probeTSV'], [6, 11], [1], subs,
                                       required_term=cfg['probeTypeLM'])

    existingProbeColorIds = columns_to_dict(cfg['probeColorTSV'], [4, 3], [1], required_term='LMPC')

    # Make adjustments for formatting inconsistencies and make a dictionary of all column names in the tsv files.
    row = open(cfg['metadataFileLM']).readline()
    row = row.strip().split("\t")
    row = [re.sub(' ', '', x) for x in row] # remove spaces in the column names
    row = [''.join(c for c in str(x) if ord(c) < 128) for x in row]  # remove characters added by encoding conversion
    # cd = makeIndexDict(row)  # column dictionary
    cd = make_index_dict(row)

    # Open tsv files
    sf = open(cfg['sampleTSV'], "a+")
    pf = open(cfg['probeTSV'], "a+")
    pcf = open(cfg['probeColorTSV'], "a+")
    imgf = open(cfg['imgTSV'], "a+")
    imgft = open(cfg['imgTSV'] + '.tmp', "a+")
    sff = open(cfg['suppTSV'], "a+")
    aef = open(cfg['analysisEntityTSV'], "a+")

    # Loop through the metadata file and update tsv files with the information
    with open(cfg['metadataFileLM']) as mdf:
        for line in mdf:
            # Parse the metadata file row into a list
            line = line.strip().split("\t")

            if line == [""] or line[0].lower() == "filename":
                # print ('continued')
                continue
            filename = line[0]
            line = [x if x != "blank" else "" for x in line]  # replace blanks with empty string

            # age = line[cd['CHRONOLOGICAL_AGE_sa']]
            age = line[cd['age_img']]
            age = re.sub("PND", "P", age)
            age = re.sub("Day", "day", age)
            age = re.sub("Month", "month", age)
            age = re.sub("Year", "year", age)
            age = re.sub("[\-\s]Old", "", age)
            age = re.sub("\s$", "", age)
            age = re.sub("^\s", "", age)
            age = re.sub("(?<!^1\s)year(?!s)", "years", age)
            age = re.sub("(?<!^1\s)month(?!s)", "months", age)
            age = re.sub("(?<!^1\s)day(?!s)", "days", age)
            # age = getFromfile(cfg['ontologyFile'], age, )
            age = getFromFile(cfg['ontologyFile'], age, ontology_regex_template)

            # Get expID and imgID
            imgId_img = getColValue(cfg['xrefFile'], filename, 1)
            expId_img = getColValue(cfg['xrefFile'], filename, 2)

            if imgId_img == "":
                filename = re.sub(" ", "", filename)
                imgId_img = getColValue(cfg['xrefFile'], filename, 1)
                expId_img = getColValue(cfg['xrefFile'], filename, 2)

            # Get the external (researcher assigned) sample id and update sample file if sample does not already exist
            externalSampleId = line[cd['LOCAL_ID_sa']]

            # if externalSampleId.lower() not in existingSampleIds.values():
            if externalSampleId.lower() not in existingSampleIds:
                LMsampleId = "LMSP" + ('0' * (10 - len(str(nextSampleNumber)))) + str(nextSampleNumber)
                nextSampleNumber += 1

                # strain = getFromControlledVocab(line[cd['STRAIN_sa']])
                # genotype = getFromControlledVocab(line[cd['GENOTYPE_sa']])
                strain = getFromFile(cfg['controlledVocab'], line[cd['STRAIN_sa']], controlled_vocab_regex_template)
                genotype = getFromFile(cfg['controlledVocab'], line[cd['GENOTYPE_sa']], controlled_vocab_regex_template)
                # genotype = getFromControlledVocab(line[cd['GENOTYPE_sa']])
                sex = re.sub("^\s", "", line[cd['SEX_sa']])
                sex = getFromFile(cfg['controlledVocab'], sex, controlled_vocab_regex_template)

                newRow = "\t".join([line[cd['row_type_sa']], LMsampleId.upper(), line[cd['type_sa']], externalSampleId,
                                    line[cd['TAXON_ID_sa']], age, sex, line[cd['BODY_WEIGHT_sa']], line[cd['RACE_sa']],
                                    line[cd['CAUSE_OF_DEATH_sa']], line[cd['HEALTH_STATUS_sa']], line[cd['GA_AT_BIRTH_sa']],
                                    line[cd['CGA_sa']], line[cd['WEIGHT_PERCENTILE_sa']], line[cd['TYPE_OF_DEATH_sa']],
                                    strain, genotype, line[cd['CROWN_RUMP_LENGTH_sa']], line[cd['HARVEST_DATE_sa']]]) + "\n"
                sf.write(newRow)
                logging.debug(newRow)

                existingSampleIds[externalSampleId] = LMsampleId
                # existingSampleIds[externalSampleId.lower()] = LMsampleId

            # Update analysis entities if necessary
            LMaeId = ''
            if line[cd['start_mz_ae']] != '' and line[cd['start_mz_ae']] != 'blank':

                mz_range = '{0}|{1}'.format(line[cd['start_mz_ae']], line[cd['end_mz_ae']])

                # if mz_range not in existingMZRanges:
                if mz_range not in existingMZRanges.keys():
                    LMaeId = "LMAE" + ('0' * (10 - len(str(nextAeNumber)))) + str(nextAeNumber)
                    nextAeNumber += 1
                    if line[cd['mode_ae']] == 'POS':
                        mode = 'LMCV0000000088; Positive'
                    elif line[cd['mode_ae']] == 'NEG':
                        mode = 'LMCV0000000088; Negative'
                    newRow = "\t".join([line[cd['row_type_ae']], LMaeId, line[cd['type_ae']], expId_img,
                                        line[cd['label_ae']], mode, line[cd['exact_mz_ae']],
                                        line[cd['start_mz_ae']], line[cd['end_mz_ae']],
                                        line[cd['mapping_ae']]]) + "\n"
                    aef.write(newRow)
                    logging.debug(newRow)
                    # existingMZRanges[LMaeId] = mz_range
                    existingMZRanges[mz_range] = LMaeId
                elif mz_range in existingMZRanges.keys():
                    LMaeId = existingMZRanges[mz_range]


            # Get probe ids and add to probe.tsv and probe_color.tsv if they do not already exist
            externalProbeIds = line[cd['label_p']]

            if cfg['experimentType'] == 'IF' and cfg['probeTypeLM'] != 'NA':

                # subs = {'nkx2-1': 'nkx2.1', 'vimentin': 'vim', "anti\-": ""}
                # existingProbeIds = columns_to_dict(cfg['probeTSV'], [1, 6], [11], subs, required_term = cfg['probeTypeLM'])
                # existingProbeIds = columns_to_dict(cfg['probeTSV'], [6,11], [1], subs,
                #                                    required_term=cfg['probeTypeLM'])
                # # print (existingProbeIds)
                # # existingProbeColorIds = columns_to_dict(cfg['probeColorTSV'], [1, 4], [3], required_term = 'LMPC')
                # existingProbeColorIds = columns_to_dict(cfg['probeColorTSV'], [4,3], [1], required_term='LMPC')

                if len(externalProbeIds.split("|")) == 1:
                    # probeColors = probeColorDict[externalProbeIds]
                    externalProbeIds = externalProbeIds.lower()
                    for sub in subs:
                        externalProbeIds = re.sub(sub, subs[sub], existingProbeIds)
                    # externalProbeInfos = line[cd['probe_info']].lower()
                    entrezIds = line[cd['target_molecule_p']]
                    probeColors = line[cd['color_pc']]


                elif len(externalProbeIds.split("|")) > 1:
                    probeColors = probeColorDict[externalProbeIds].split("|")
                    externalProbeIds = externalProbeIds.lower().split("|")
                    externalProbeInfos = line[cd['probe_info']].lower().split("|")
                    entrezIds = line[cd['target_molecule_p']].split("|")

                # LMprobeIds = line[cd['label_p']]
                LMprobeColorIds = line[cd['probe_color_img']]

                if LMprobeColorIds == "":

                    # update probe file if probe does not already exist
                    # probeIdsTuples = []
                    # for probeIdWithInfo in existingProbeIds.keys():
                    #     probeId, info = probeIdWithInfo.split("|")
                    #     probeIdsTuples += [(probeId, info)]
                    #
                    # probeColorIdsTuples = []
                    # for probeColorIdWithInfo in existingProbeColorIds.keys():
                    #     probeId, color = probeColorIdWithInfo.split("|")
                    #     probeIdsTuples += [(probeId, color)]

                    LMprobeColorIds = ""
                    LMprobeIds = ""
                    # LMprobeColorIds = []
                    # LMprobeIds = []
                    for index, externalProbeId in enumerate(externalProbeIds):
                        externalProbeInfo = externalProbeInfos[index]
                        if externalProbeInfo == "":
                            continue
                        entrezId = entrezIds[index]
                        probeColor = probeColors[index]
                        # indices = [i for i, x in enumerate(probeIdsTuples) if x[0] == externalProbeId]
                        # existingProbeInfos = []
                        # existingProbeInfoInExternalInfo = False
                        color = probeColors[index]
                        # for i in indices:
                        #     if probeIdsTuples[i][1] in externalProbeInfo:
                        #         existingProbeInfoInExternalInfo = True
                        # probe_color_combo = externalProbeId + "|" + probeColor

                        LMprobeId_p = matchProbeId(existingProbeIds, externalProbeId, externalProbeInfo)
                        if LMprobeId_p == "":
                            LMprobeId_p = cfg['probeTypeLM'] + ('0' * (10 - len(str(nextProbeNumber)))) + str(nextProbeNumber)
                            nextProbeNumber += 1
                            productNum = ""
                            manufacturer = ""

                            if len(externalProbeInfo.split(", ")) > 1:
                                productNum = externalProbeInfo.split(", ")[0]
                                manufacturer = externalProbeInfo.split(", ")[-1]

                            if line[cd['template_seq']] != "":
                                ff = open("/data/processing/carson/2017_07_10/processing/fasta_files/" + LMprobeId_p +
                                          ".fasta", "w+")
                                logging.debug(">template_seq\n" + line[cd['template_seq']] + "\n>primer_frw\n" +
                                      line[cd['primer_fwd']] + "\n>primer_rev\n" + line[cd['primer_rev']] + "\n")
                                fastaPath = "https://lungmap-breath-data.s3.amazonaws.com/" + LMprobeId_p + ".fasta"

                                suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
                                nextSuppNumber += 1

                                files_p = suppId
                                type_sf = "Probe & Primer Sequences"
                                file_caption_sf = ""
                                file_type_sf = "probe_sequences"
                                newRowSF = "\t".join(
                                    [line[cd['row_type_sf']], suppId, type_sf, type_sf, fastaPath, file_caption_sf,
                                     file_type_sf]);
                                sff.write(newRowSF + "\n")
                                logging.debug(newRowSF + "\n")

                            newRow = "\t".join([line[cd['row_type_p']], LMprobeId_p, line[cd['type_p']],
                                                line[cd['probe_type_p']], line[cd['target_condition_p']],
                                                line[cd['data_id_p']], externalProbeId.upper(), entrezId,
                                                entrezId, line[cd['dilution_int_p']], line[cd['dilution_str_p']],
                                                productNum.upper(), manufacturer.upper(), line[cd['product_url_p']],
                                                line[cd['comment_p']], line[cd['files_p']]])

                            existingProbeIds[externalProbeId + "|" + productNum] = LMprobeId_p
                            pf.write(newRow + "\n")
                            logging.debug(newRow + "\n")

                        LMprobeColorId_p = matchProbeId(existingProbeColorIds, LMprobeId_p.lower(), probeColor.lower())
                        # if LMprobeColorId_p == "" and LMprobeId_p + "|" + color not in existingProbeColorIds:
                        if LMprobeColorId_p == "" and LMprobeId_p + "|" + color not in existingProbeColorIds.keys():
                            LMprobeColorId_p = "LMPC" + ('0' * (10 - len(str(nextProbeColorNumber)))) + str(nextProbeColorNumber)
                            nextProbeColorNumber += 1
                            newColorRow = "\t".join([line[cd['row_type_pc']], LMprobeColorId_p, line[cd['type_pc']],
                                                     probeColor, LMprobeId_p])

                            existingProbeColorIds[LMprobeId_p + "|" + color] = LMprobeColorId_p
                            pcf.write(newColorRow + "\n")
                            # logging.debug(newColorRow + "\n")

                        # probeIdsTuples += [(externalProbeId, externalProbeInfo)]
                        LMprobeIds = LMprobeIds + "|" + LMprobeId_p
                        LMprobeColorIds = LMprobeColorIds + "|" + LMprobeColorId_p

                        # if externalProbeId == "Blank" or externalProbeId == "x" or "Blank" in filename:
                        #     LMprobeId_img = ""
                        #     LMprobeColorId_img = ""
                        # else:
                        #     LMprobeId_img = matchProbeId(existingProbeIds, externalProbeId, externalProbeInfo)

            if cfg['experimentType'] == 'ISH' and cfg['probeTypeLM'] != 'NA':

                # existingProbeIds = getExistingIDsDict(cfg['probeTSV'], 1, 6, 'na', cfg['probeTypeLM'])
                # existingProbeColorIds = getExistingIDsDict(cfg['probeColorTSV'], 1, 4, 3, "LMPC")
                existingProbeIds = columns_to_dict(cfg['probeTSV'], [6], [1], required_term = cfg['probeTypeLM'])
                # existingProbeColorIds = {}
                existingProbeColorIds = columns_to_dict(cfg['probeColorTSV'], [4, 3], [1])
                LMprobeIds = ""
                LMprobeColorIds = ""

                # update probe file if probe does not already exist
                if externalProbeIds.lower() not in existingProbeIds.keys():

                    LMprobeId_p = cfg['probeTypeLM'] + ('0' * (10 - len(str(nextProbeNumber)))) + str(nextProbeNumber)
                    nextProbeNumber += 1
                    LMprobeColorId_p = "LMPC" + ('0' * (10 - len(str(nextProbeColorNumber)))) + str(nextProbeColorNumber)
                    nextProbeColorNumber += 1
                    #            if line[cd['template_seq']] != "blank":
                    if line[cd['template_seq']] != "":
                        ff = open("/data/processing/carson/2017_07_10/processing/fasta_files/" + LMprobeId_p + ".fasta",
                                  "w+")
                        ff.write(">template_seq\n" + template_p + "\n>primer_frw\n" + fwdPrimer_p + "\n>primer_rev\n" + revPrimer_p + "\n")
                        logging.debug(">template_seq\n" + line[cd['template_seq']] + "\n>primer_frw\n" + line[
                            cd['primer_fwd']] + "\n>primer_rev\n" + \
                              line[cd['primer_rev']] + "\n")
                        fastaPath = "https://lungmap-breath-data.s3.amazonaws.com/" + LMprobeId_p + ".fasta"

                        suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
                        nextSuppNumber += 1
                        files_p = suppId
                        type_sf = "Probe & Primer Sequences"
                        file_caption_sf = ""
                        file_type_sf = "probe_sequences"
                        newRowSF = "\t".join(
                            [line[cd['row_type_sf']], suppId, type_sf, type_sf, fastaPath, file_caption_sf,
                             file_type_sf]);
                        sff.write(newRowSF + "\n")
                        logging.debug(newRowSF + "\n")

                    newRow = "\t".join(
                        [line[cd['row_type_p']], LMprobeId_p, line[cd['type_p']], line[cd['probe_type_p']],
                         line[cd['target_condition_p']],
                         line[cd['data_id_p']], line[cd['label_p']], line[cd['target_molecule_p']],
                         line[cd['mapping_p']], line[cd['dilution_int_p']],
                         line[cd['dilution_str_p']], line[cd['product_num_p']], line[cd['manufacturer_p']],
                         line[cd['product_url_p']],
                         line[cd['comment_p']], line[cd['files_p']]])

                    newColorRow = "\t".join(
                        [line[cd['row_type_pc']], LMprobeColorId_p, line[cd['type_pc']], line[cd['color_pc']],
                         LMprobeId_p])

                    if externalProbeIds != "Blank" and externalProbeIds != "x" and "Blank" not in filename:
                        pf.write(newRow + "\n")
                        pcf.write(newColorRow + "\n")
                        logging.debug(newRow + "\n")
                        logging.debug(newColorRow + "\n")
                        existingProbeIds[externalProbeIds.lower()] = LMprobeId_p
                        existingProbeColorIds[LMprobeId_p] = LMprobeColorId_p
                        LMprobeIds = existingProbeIds[externalProbeIds]
                        LMprobeColorIds = existingProbeColorIds[LMprobeIds]
                    # else:
                    #     LMprobeId_img = ""
                    #     LMprobeColorId_img = ""
                    #
                    #     LMprobeIds = ""
                    #     LMprobeColorIds = ""
                    # if externalProbeIds == "Blank" or externalProbeIds == "x" or "Blank" in filename:

                    # else:
                    #     LMprobeIds = existingProbeIds[externalProbeIds]
                    #     LMprobeColorIds = existingProbeColorIds[LMprobeIds]

            # Update expression_image.tsv


            directory_img = imgId_img
            sampleId_img = existingSampleIds[externalSampleId.lower()].upper()
            # sampleId_img = externalSampleId

            # anatomyId_img = getFromOntology(line[cd['anatomy_terms_exp']])  # or anatomy_ids_img column can be used
            anatomyId_img = getFromFile(cfg['ontologyFile'], line[cd['anatomy_terms_exp']], ontology_regex_template)  # or anatomy_ids_img column can be used

            default_img = str('Tile' in filename)

            xScaling_img = getColValue(cfg['scalingFactors'], imgId_img, 1)
            yScaling_img = getColValue(cfg['scalingFactors'], imgId_img, 2)

            filename_no_dir = filename.split('/')[-1]
            # update supporting files
            imgPath_sf = cfg['path_prefix_sf'] + expId_img + "/" + imgId_img + "/" + filename_no_dir + ".gz"
            thumbPath_sf = cfg['path_prefix_sf'] + expId_img + "/" + imgId_img + "/" + imgId_img + ".thumb.tif"
            homePath_sf = cfg['path_prefix_sf'] + expId_img + "/" + imgId_img + "/" + imgId_img + ".home.tif"
            tilePath_sf = cfg['path_prefix_sf'] + expId_img + "/" + imgId_img + "/tiles"


            if "Blank" in filename:
                imgFileType = "negative_control"
                # fileCaption = xScaling_img + "___" + yScaling_img
                imgFileCaption = xScaling_img + "___" + yScaling_img

            elif "Gyg" in filename:
                imgFileType = "positive_control"
                # fileCaption = xScaling_img + "___" + yScaling_img
                imgFileCaption = xScaling_img + "___" + yScaling_img

            else:
                imgFileType = cfg['imgFileType_sf']
                fileCaption = xScaling_img + "___" + yScaling_img
                imgFileCaption = xScaling_img + "___" + yScaling_img
            # if "Gyg" in filename:
            #     imgFileType = "positive_control"
            #     fileCaption = xScaling_img + "___" + yScaling_img

            imgFileCaption = ""
            if xScaling_img != "" and "control" in cfg['imgFileType_sf']:
                tileFileCaption = xScaling_img + "___" + yScaling_img
                thumbFileCaption = xScaling_img + "___" + yScaling_img
            else:
                tileFileCaption = ""
                thumbFileCaption = ""
                homeFileCaption = ""

            suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
            # newImgRow = "\t".join([line[cd['row_type_sf']], suppId, line[cd['type_sf']], cfg['imgFileLabel_sf'],
            #                        imgPath_sf, imgFileCaption, cfg['imgFileType_sf']])
            newImgRow = "\t".join([line[cd['row_type_sf']], suppId, line[cd['type_sf']], cfg['imgFileLabel_sf'],
                                   imgPath_sf, imgFileCaption, imgFileType])
            originalSuppId = suppId
            nextSuppNumber = nextSuppNumber + 1
            suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
            newThumbRow = "\t".join([line[cd['row_type_sf']], suppId, line[cd['type_sf']], cfg['thumbFileLabel_sf'], thumbPath_sf, thumbFileCaption, cfg['thumbFileType_sf']])

            nextSuppNumber += 1
            suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
            newHomeRow = "\t".join([line[cd['row_type_sf']], suppId, line[cd['type_sf']], cfg['homeFileLabel_sf'],
                                    homePath_sf, homeFileCaption, cfg['homeFileType_sf']])
            nextSuppNumber += 1
            suppId = "LMSF" + ('0' * (10 - len(str(nextSuppNumber)))) + str(nextSuppNumber)
            newTileRow = "\t".join([line[cd['row_type_sf']], suppId, line[cd['type_sf']], cfg['tileFileLabel_sf'],
                                    tilePath_sf, tileFileCaption, cfg['tileFileType_sf']])
            nextSuppNumber = nextSuppNumber + 1

            sff.write(newImgRow + "\n" + newThumbRow + "\n" + newHomeRow + "\n" + newTileRow + "\n")
            logging.debug(newImgRow + "\n" + newThumbRow + "\n" + newHomeRow + "\n" + newTileRow + "\n")

            newRow_img = [line[cd['row_type_img']], imgId_img, line[cd['type_img']], originalSuppId, directory_img,
                          expId_img, line[cd['date_img']], sampleId_img, line[cd['magnification_img']],
                          line[cd['platform_img']], LMprobeColorIds, anatomyId_img, xScaling_img, yScaling_img, age,
                          LMaeId, default_img]

            imgf.write("\t".join(newRow_img[:16]) + "\n")
            imgft.write("\t".join(newRow_img) + "\n")
            logging.debug("\t".join(newRow_img) + "\n")

    sff.close()
    mdf.close()
    sf.close()
    pf.close()
    pcf.close()
    imgf.close()
    imgft.close()

    # update experiment file
    expf = open(cfg['expTSV'], "a+")
    for expId in newExpIds:

        sampleIds_exp = []
        probeColors_exp = []
        anatomyIds_exp = []
        ages_exp = []
        suppFiles_exp = []

        s3_path_exp = line[cd['s3_path_exp']] + expId

        defaultImageId_exp = ""
        default_exp = ""
        img_counter = 0
        with open(cfg['imgTSV'] + '.tmp') as imgft:
            for img_line in imgft:

                img_line = img_line.strip().split("\t")
                if img_line[0] != 'data' or img_line[5] != expId:
                    continue
                img_counter += 1
                imgId_exp = img_line[1]
                rowDefault_exp = img_line[16]
                if rowDefault_exp == 'True' or img_counter == 1:
                    defaultImageId_exp = imgId_exp
                    default_exp = imgId_exp

                sampleIds_exp += [img_line[7]]
                probeColors_exp += [img_line[10]]
                anatomyIds_exp += [img_line[11]]
                ages_exp += [img_line[14]]
                suppFiles_exp += [img_line[3]]
        imgf.close()
        sampleIds_exp = "|".join(set(sampleIds_exp))
        probeColors_exp = "|".join(set(probeColors_exp))
        anatomyIds_exp = "|".join(set(anatomyIds_exp))
        ages_exp = "|".join(set(ages_exp))
        suppFiles_exp = "|".join(set(suppFiles_exp))

        newRow = "\t".join([line[cd['row_type_exp']], expId, line[cd['type_exp']], line[cd['submission_stage_exp']],
                            line[cd['experiment_type_exp']], line[cd['release_date_exp']], line[cd['taxon_exp']],
                            line[cd['researchers_exp']], line[cd['sites_exp']], line[cd['sample_count_exp']],
                            line[cd['label_exp']], line[cd['comment_exp']], ages_exp, anatomyIds_exp, sampleIds_exp,
                            suppFiles_exp, s3_path_exp, default_exp, defaultImageId_exp, probeColors_exp,
                            line[cd['id_type_exp']], line[cd['sample_type_exp']], line[cd['technology_exp']]])
        expf.write(newRow + "\n")
        logging.debug(newRow + "\n")

    expf.close()

    os.remove(cfg['imgTSV'] + '.tmp')
    if cfg['probeTypeLM'] != 'NA':
        os.remove(cfg['probeTSV'] + '.tmp')

    # Check how many row were added to each of the tsv files
    for e in ['expression_image', 'experiment', 'sample', 'probe', 'probe_color', 'supporting_file', 'analysis_entity']:
        check_tsv_cmd = r'''echo "\n$(( `wc -l < tsv/{0}.tsv` - `wc -l < {1}/{0}_backup.tsv`  )) row(s) added to {0}.tsv"'''.format(e, 'tsv', )
        logging.info(run_shell_cmd(check_tsv_cmd))

    logging.info("done updating tsv files")

#####################
# Summarize results of image processing
def summarize():
    logging.info('Summarizing image processing results')
    cmd = r"pcregrep -o '2[^\n]*\.(tif|jpg)' {0} | sort --unique | wc -l".format(cfg['xrefFile'])
    cmd = r"pcregrep -o '2[^\n]*(\.tif|\.jpg)?' {0} | sort --unique | wc -l".format(cfg['xrefFile'])
    logging.info(re.sub('\s', '', run_shell_cmd(cmd)) + " image(s) in xref file")

    cmd = r"pcregrep -o '^LMIM[\d]{10}(?!\n)' " + cfg['scalingFactors'] + " | sort --unique | wc -l"

    logging.info(run_shell_cmd(cmd) + " image(s) in scaling factors file")
    missing_files_log_file = cfg['processingDir'] + 'images_missing_files.txt'
    missing_files_metadata_file = cfg['processingDir'] + 'images_missing_files_metadata.txt'

    ### Go through the xref file and check that each of the images has a thumbnail image, home image,
    ### zipped original image, and tiles in the associated folder.
    missing_files_log = open(missing_files_log_file, "w")
    missing_files_metadata = open(missing_files_metadata_file, "w")
    missing_files_count = 0
    images_missing_files_count = 0


    with open(cfg['xrefFile']) as xf:
        for line in xf:
            file_with_dir, imgId, expId = line.strip().split('\t')
            file = file_with_dir.split('/')[-1]
            suffix = file.split('.')[-1]
            all_files_exist = True
            processed_files = ['{0}.gz'.format(file), '{0}.home.{1}'.format(imgId, suffix), '{0}.thumb.{1}'.format(imgId, suffix), 'tiles']

            for processed_file in processed_files:
                if not path.exists(
                        '{0}lungmap_breath_data/{1}/{2}/{3}'.format(cfg['processingDir'], expId, imgId, processed_file)):
                    missing_files_log.write(
                        '{0}lungmap_breath_data/{1}/{2}/ missing {3}.gz\n'.format(cfg['processingDir'], expId, imgId,
                                                                                  processed_file))
                    missing_files_count += 1
                    all_files_exist = False

            if all_files_exist == False:
                images_missing_files_count += 1
                row = my_grep(file, cfg['metadataFileLM'], 0)
                missing_files_metadata.write(row + '\n')

    logging.info('{0} image folders are missing files'.format(images_missing_files_count))
    logging.info('{0} files are missing'.format(missing_files_count))


#####################
# Reset tsv files to original
def reset_tsv():
    logging.info('Resetting tsv files')
    # Reset tsv files to original version
    reset_tsv_cmd = '''
        cp {0}sample_original.tsv {0}sample.tsv
        cp {0}supporting_file_original.tsv {0}supporting_file.tsv
        cp {0}probe_original.tsv {0}probe.tsv
        cp {0}probe_color_original.tsv {0}probe_color.tsv
        cp {0}expression_image_original.tsv {0}expression_image.tsv
        cp {0}experiment_original.tsv {0}experiment.tsv
        cp {0}analysis_entity_original.tsv {0}analysis_entity.tsv
        '''.format(cfg['tsvDir'])
    run_shell_cmd(reset_tsv_cmd)


def main():
    if args.pre_metadata is True:
        create_pre_metadata()

    if args.LM_metadata is True or args.ids is True:
        filenameList = create_filnames()

    if args.LM_metadata is True:
        create_LM_metadata(filenameList)

    if args.ids is True:
        assign_ids(filenameList)

    if args.process is True:
        process_images()
        summarize()

    if args.tsv == True:
        update_tsv()

    if args.reset is True:
        reset_tsv()


if __name__ == '__main__':
    main()
    logging.info('done')
