import os
from os import listdir
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time


# Results Directories and filenames
clientListDir = r'\\companyNetworkDrive\ROC\ConflictCheck\ClientList'
resultDir = r'\\companyNetworkDrive\ROC\Structures'
resFilename = 'structures.xlsx'
tempFileDir = r'\\companyNetworkDrive\ROC\Structures\temp'
amlReportFilename = 'AMLreport.xlsx'
errorList = []
errorFile = 'error.xlsx'
archive = []
archiveFile = 'archive.xlsx'

# Data Source
sourceAML = r'\\companyNetworkDrive2\Risk\Documents\AML'
xlsm = '.xlsm'
# move back result modification date to avoid loosing data
delta = timedelta(minutes=10)

crmIDs = []

# open result file
def open_result_file(resultDir, resFilename):
    print('Load workbook')
    df_structures = pd.read_excel(os.path.join(resultDir, resFilename), sheet_name='Structures')
    print('Workbook loaded')
    return df_structures


# create list of all filenames tracked by result file
def create_list_tracked_files(df_structures):
    print('Create list of tracked filenames')
    mask = df_structures['role'] == 'Client'
    df = df_structures[mask]
    trackedFilenames = df['filename'].tolist()
    print('List created')
    return trackedFilenames


# create list of all xlsm files in source directory
# create dictionary of filenames and modification dates and sort by mod date
def create_list_source_files(sourceAML, xlsm):
    print('Create list of AML\'s in source directory')
    filenamesInSource = []
    amlDictionary = {}
    count = 0
    target = 1000
    for aml in listdir(sourceAML):
        count += 1
        if aml.endswith(xlsm):
            amlModDate = datetime.fromtimestamp(os.path.getmtime(os.path.join(sourceAML, aml)))
            amlDictionary[aml] = amlModDate
            filenamesInSource.append(aml)
            if count > target:
                print(count)
                target += 1000

    print('Sorting...')
    sortedAML = sorted(amlDictionary.items(), key=lambda t: t[1], reverse=True)
    print('List created')
    return filenamesInSource, sortedAML


# create list of aml files to delete from result file
# call delete_aml() function on each one on the list
def amls_to_delete(df_structures, trackedFilenames, filenamesInSource):
    toDelete = list(set(trackedFilenames) - set(filenamesInSource))
    print('Filenames to delete')
    num = 0
    for pos in toDelete:
        num += 1
        ifDeleted, df_structures = delete_aml(df_structures, pos)
        if ifDeleted:
            print(str(num) + ' - ' + pos + ' deleted')
        else:
            print(str(num) + ' - ' + pos + ' delete error')
    return df_structures


def delete_aml(df_structures, pos):
    try:
        mask = (df_structures['filename'] != pos)
        df_structures = df_structures[mask]
        archive.append(pos)
        return True, df_structures
    except KeyError:
        err = pos + ' - aml delete error'
        errorList.append(err)
    return False, df_structures


# create list of all crm ids tracked by result file
def create_list_crm(df_structures):
    print('Create list of tracked crm IDs')
    mask = df_structures['role'] == 'Client'
    df = df_structures[mask]
    crmList = df['crm_id'].tolist()
    print('List created')
    return crmList


# Get the last modification time of the result file
def get_result_mod_date(resultDir, resFilename, delta):
    print('Getting result file modification date')
    resModDate = datetime.fromtimestamp(os.path.getmtime(os.path.join(resultDir, resFilename)))
    resModDate -= delta
    return resModDate


# Create list of all aml files modified after result file
def create_list_files_to_add(resultDir, resFilename, sortedAML, delta):
    resModDate = get_result_mod_date(resultDir, resFilename, delta)
    filenamesToAdd = []
    num = 0
    for f, modDate in sortedAML:
        if(modDate > resModDate):
            num += 1
            filenamesToAdd.append(f)
            print(str(num) + ' - ' + f)
        else:
            break
    filenamesToAdd.reverse()
    print('List of files to add completed')
    return filenamesToAdd


def read_structure(sourceAML, filename):
    columns = ['Entity name', 'Ownership', 'Legal form', 'Percentage in ownership', 'Country ISO', 'Type of entity',
               'Listed or quoted', 'Stock Exchange', 'Comment']

    structureAll = pd.read_excel(os.path.join(sourceAML, filename), sheet_name='Structure')
    structureTemp = pd.read_excel(os.path.join(sourceAML, filename), sheet_name='Structures', header=14, usecols=columns)
    clientData = structureTemp[0:1]
    structure = structureTemp[pd.notnull(structureTemp['Ownership'])]
    crmId = structureAll.iloc[4, 5]
    clientName = structureAll.iloc[4, 1]
    modDate = datetime.fromtimestamp((os.path.getmtime(os.path.join(sourceAML, filename))))
    creationDate = datetime.fromtimestamp((os.path.getctime(os.path.join(sourceAML, filename))))

    return clientName, crmId, structure, modDate, creationDate, clientData


def update_df(sourceAML, df_structures, filenamesToAdd, crmList):
    num = 0
    for aml in filenamesToAdd:
        num += 1
        try:
            df_structures, crmList = update(num, sourceAML, aml, df_structures, crmList)
        except KeyError:
            err = aml + ' - aml update error'
            errorList.append(err)
        except ValueError:
            err = aml + ' - aml update error'
            errorList.append(err)
        except PermissionError:
            err = aml + ' - aml update error'
            errorList.append(err)
        except TypeError:
            err = aml + ' - aml update error'
            errorList.append(err)
        except IOError:
            err = aml + ' - aml update error'
            errorList.append(err)
    return df_structures


def update(num, sourceAML, aml, df_structures, crmList):
    #read data from AML
    s = read_structure(sourceAML, aml)
    if s[1] in crmIDs:
        try:
            mask = (df_structures['role'] == 'Client') & (df_structures['crmId'] == s[1])
            archiveFilename = df_structures[mask].iloc[0, 10]
            archive.append(archiveFilename)
            # delete aml in df
            ifDeleted, df_structures = delete_aml(df_structures, archiveFilename)
            if ifDeleted:
                print(str(num) + ' - Update ' + aml)
            else:
                print('Delete error ' + archiveFilename)
        except KeyError:
            err = archiveFilename + ' - aml delete error'
            errorList.append(err)
    else:
        print(str(num) + ' - Add ' + aml)

    df_structures = add_aml(df_structures, aml, s)
    crmList.append(s[1])
    return df_structures, crmList


def add_aml(df_structures, aml, s):
    dataToAdd = []
    if s[1] in crmIDs:
        print('Skip - ' + aml)
    else:
        structureComplete = s[2].assign(filename=aml, type='AML', crm_id=s[1], modification_date=s[3], creation_date=s[4],
                                        role='Owner')
        clientListCompl = s[5].assign(filename=aml, type='AML', crm_id=s[1], modification_date=s[3], creation_date=s[4],
                                      role='Client')
        dataToAdd.append(clientListCompl)
        dataToAdd.append(structureComplete)
        df_toAdd = pd.concat(dataToAdd, sort=False, ignore_index=True)
        df_structures = df_structures.append(df_toAdd, ignore_index=True)
        dataToAdd.clear()
        crmIDs.append(s[1])
    return df_structures


def create_result_file(sourceAML, sortedAML):
    print('Create dataframe')
    col = ['Entity name', 'Ownership', 'Legal form', 'Percentage in ownership', 'Country ISO', 'Type of entity',
           'Listed or quoted', 'Stock Exchange', 'Comment', 'filename', 'type', 'crm_id', 'modification_date',
           'creation_date', 'role']
    df_structures = pd.DataFrame(None, columns=col)
    print('Looping through files i source folder and adding data to dataframe')
    num = 0
    for aml, _ in sortedAML:
        num += 1
        try:
            s = read_structure(sourceAML, aml)
            df_structures = add_aml(df_structures, aml, s)
            print(str(num) + ' - done - ' + aml)
        except KeyError:
            err = aml + ' - aml add error'
            errorList.append(err)
        except ValueError:
            err = aml + ' - aml add error'
            errorList.append(err)
        except PermissionError:
            err = aml + ' - aml add error'
            errorList.append(err)
        except TypeError:
            err = aml + ' - aml add error'
            errorList.append(err)
        except IOError:
            err = aml + ' - aml add error'
            errorList.append(err)

    print('Dataframe created')
    return df_structures


def prepare_client_list_report(df_structures):
    clientListFilename = 'Client List - ' + str(datetime.now().strftime('%d.%m.%Y')) + '.xlsx'
    df_clientList = df_structures
    df_clientList['Entity name'] = df_clientList['Entity name'].fillna(df_clientList['Ownership'])

    colToStay = ['Entity name', 'Ownership', 'crm_id', 'role', 'Legal form', 'Percentage in ownership', 'Country ISO',
                 'Type of entity', 'filename', 'type', 'modification_date', 'creation_date']
    df_clientList = df_clientList[colToStay]
    return  df_clientList, clientListFilename


def save(df_structures, resFilename, resultDir, df_clientList, clientListFilename, clientListDir, errorList, errorFile, archive, archiveFile):
    df_err = pd.DataFrame(errorList)
    print('Save ' + errorFile)
    with pd.ExcelWriter(os.path.join(resultDir, errorFile), engine='openpyxl') as writer:
        df_err.to_excel(writer, sheet_name='ErrorList', index=False)

    df_archive = pd.DataFrame(archive)
    print('Save ' + archiveFile)
    with pd.ExcelWriter(os.path.join(resultDir, archiveFile), engine='openpyxl') as writer:
        df_archive.to_excel(writer, sheet_name='ArchiveFiles', index=False)

    print('Save ' + resFilename)
    with pd.ExcelWriter(os.path.join(resultDir, resFilename), engine='openpyxl') as writer:
        df_structures.to_excel(writer, sheet_name='Structures', index=False)

    print('Save ' + clientListFilename)
    with pd.ExcelWriter(os.path.join(clientListDir, clientListFilename), engine='openpyxl') as writer:
        df_clientList.to_excel(writer, sheet_name='ClientList', index=False)




def main(sourceAML, resultDir, resFilename, xlsm, clientListDir, delta):
    start = time.time()
    ifExists = os.path.exists(os.path.join(resultDir, resFilename))
    if (ifExists):
        df_structures = open_result_file(resultDir, resFilename)
        trackedFilenames = create_list_tracked_files(df_structures)
        filenamesInSource, sortedAML = create_list_source_files(sourceAML, xlsm)
        df_structures = amls_to_delete(df_structures, trackedFilenames, filenamesInSource)
        crmList = create_list_crm(df_structures)
        filenamesToAdd = create_list_files_to_add(resultDir,resFilename, sortedAML, delta)
        df_structures = update_df(sourceAML, df_structures, filenamesToAdd, crmList)
    else:
        _, sortedAML = create_list_source_files(sourceAML, xlsm)
        df_structures = create_result_file(sourceAML, sortedAML)

    df_clientList, clientListFilename = prepare_client_list_report(df_structures)
    save(df_structures, resFilename, resultDir, df_clientList, clientListFilename, clientListDir, errorList, errorFile, archive, archiveFile)


main(sourceAML, resultDir, resFilename, xlsm, clientListDir, delta)