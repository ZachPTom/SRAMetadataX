import fire
import sqlite3
import pandas as pd
import os
import requests
import sys
import gzip
import shutil
from tqdm.autonotebook import tqdm
from collections import OrderedDict 


SRADB_URL = [
    "https://s3.amazonaws.com/starbuck1/sradb/SRAmetadb.sqlite.gz",
    "https://gbnci-abcc.ncifcrf.gov/backup/SRAmetadb.sqlite.gz",
]

SQL_dict = {'all_sm_lcp': 'SELECT experiment_accession FROM sra WHERE library_construction_protocol IS NOT NULL;',
            'all_sm_lcp_kw': 'SELECT experiment_accession FROM experiment WHERE (study_accession=?) AND (library_construction_protocol ' +
                             'IS NOT NULL);',
            'count_lcp': 'SELECT count(library_construction_protocol) FROM experiment WHERE library_construction_protocol ' + 
                          'like ? OR library_construction_protocol like ?;',
            'create_params': 'CREATE TABLE IF NOT EXISTS params (accession TEXT PRIMARY KEY, fragmentation TEXT, adapter_ligation TEXT, ' +
                             'enrichment TEXT);',
            'keyword_match': 'SELECT experiment_accession FROM sra WHERE (experiment_accession=?) AND (library_construction_protocol ' +
                             'LIKE ? OR study_abstract LIKE ?);',
            'km_create_temp': 'CREATE TEMP TABLE km_experiments AS SELECT * FROM sra WHERE 0;',
            'km_insert_temp': 'INSERT INTO km_experiments SELECT * FROM sra WHERE experiment_accession=?;',
            'meta_info': 'SELECT * FROM metaInfo;',
            'list_tables': 'SELECT name FROM sqlite_master WHERE type="table";',
            'srx_sa_lcp': 'SELECT {} FROM sra WHERE experiment_accession=?;'
            }


class SRAMetadataX(object):
    def __init__(self):
        """Initialize SRAdb.
        Parameters
        ----------
        self: string
                    Extract metadata from the SRAdb package. Output SRRs to a text file and use the SRA toolkit to download.
        sqlite_file: string
                    Path to unzipped SRAmetadb.sqlite file
        """
        # First check for database file in current directory
        if not os.path.exists(os.path.join(os.getcwd(), "SRAmetadb.sqlite")):   
            if os.path.exists('.databasepath'):
                with open('.databasepath', 'r') as f:
                    for line in f:
                        self.sqlite_file = line
        else:
            self.sqlite_file = os.path.join(os.getcwd(), "SRAmetadb.sqlite")

        try:
            self.db = sqlite3.connect(
                "file:{}?mode=rw".format(self.sqlite_file), uri=True)
        except:
            value = input(
                "SRAmetadb sqlite file not found. Download file? Enter [y/n]:\n")
            if value == 'y':
                self.download_sradb()
                self.sqlite_file = os.path.join(
                    os.getcwd(), "SRAmetadb.sqlite")
                self.db = sqlite3.connect(
                    "file:{}?mode=rw".format(self.sqlite_file), uri=True)
            else:
                value = input(
                    "Enter the path to your SRAmetadb.sqlite file (enter [n] to exit):\n")
                if value == 'n':
                    print('Exiting...')
                    exit()
                else:
                    self.sqlite_file = value
                    self.db = sqlite3.connect(
                        "file:{}?mode=rw".format(self.sqlite_file), uri=True)
                    # store the given database path for future use
                    with open('.databasepath', 'w') as f:
                        f.write(value)

        self.cursor = self.db.cursor()


    def all_sm_lcp(self, terms = 'none'):
        """
        List all SRA experiments that contain sample manipulation/library construction protocol data.\n
        Alternatively, search for experiments that contain sm/lcp data and a term or set of terms.
        :param terms: a term or list of terms that submissions need to contain. Alternatively, \n
        enter the path to a text file of term groups to search for.
        :return: experiment accession numbers
        """
        results_final = []
        if terms == 'none':
            results = self.cursor.execute(SQL_dict['all_sm_lcp']).fetchall()
            return results
        else:
            srps = self.terms(terms, 'study_accession, run_accession', print_out = False)
            srps_set = set(srps)
            unique_srps = list(srps_set)
            for srp in unique_srps:
                results = self.cursor.execute(SQL_dict['all_sm_lcp_kw'], (srp[0], )).fetchall()
                for r in results:
                    results_final.append(r[0])
            results_final = list(OrderedDict.fromkeys(results_final))
            return results_final


    def _download(self, url, file_path):
        """
        Download helper function. Method name is preceded by underscore to hide from user.
        """
        with open(file_path, "wb") as f:
            print("Downloading {}".format(file_path))
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                pbar = tqdm(total=int(r.headers['Content-Length']))
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        pbar.update(len(chunk))


    def download_sradb(self):
        """
        Download SRAdb.sqlite file.
        """
        dl_path = os.path.join(os.getcwd(), "SRAmetadb.sqlite.gz")
        dl_path_unzipped = dl_path.rstrip(".gz")

        if os.path.isfile(dl_path):
            raise RuntimeError(
                "{} already exists!".format(
                    dl_path
                )
            )
        if os.path.isfile(dl_path_unzipped):
            raise RuntimeError(
                "{} already exists!".format(
                    dl_path_unzipped
                )
            )

        try:
            self._download(SRADB_URL[0], dl_path)
        except Exception as e:
            # Try NCBI
            sys.stderr.write(
                "Could not use AWS s3 {}.\nException: {}.\nTrying NCBI...\n".format(
                    SRADB_URL[0], e
                )
            )
            try:
                self._download(SRADB_URL[1], dl_path)
            except Exception as e:
                sys.stderr.write(
                    "Could not use NCBI {}.\nException: {}.\nPlease download the SQlite file via wget...\n".format(
                        SRADB_URL[0], e
                    )
                )

        print("Extracting {} ...".format(dl_path))

        with gzip.open(dl_path, "rb") as f_in:
            with open(dl_path_unzipped, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        print("SRAmetadb file download complete!")
        metadata = self.cursor.execute(SQL_dict['meta_info']).fetchall()
        print("SRAdb file Metadata:")
        print(metadata)


    def keyword_match(self, experiments_file, keyword_file = 'oogabooga', save: str = 'true'):
        """
        Search the metadata of a given list of experiments for matching keywords. Use this method \n
        as a way to parse the reagents, kits, and sm/lcp methods from desired entries. \n
        Stores experiments and associated keywords in the parameters table.
        :param experiments_file: path to user defined text file of experiments.
        :param keyword_file: path to user defined csv file of keywords
        :param save: OPTIONAL: by default stores experiments and associated keywords in database \n
        in a table called parameters. Enter 'ns' if you do not wish to store keywords.
        :return: experiments and their associated keywords
        """
        #create the params table if it does not exist
        self.cursor.execute(SQL_dict['create_params'])

        with open(experiments_file, 'r') as f:
            #create smaller table of desired experiments from sra table to speed up execution
            self.cursor.execute(SQL_dict['km_create_temp'])
            for accession in f:
                accession = accession.rstrip("\n")
                self.cursor.execute(SQL_dict['km_insert_temp'], (accession,))
                #print(self.query('SELECT count(experiment_accession) FROM km_experiments;'))

        # with open(keyword_file, 'r') as kw:
        #     for submission in s:
        #         result_string = submission
        #         sys.stdout.write(
        #             "Parsing {}.. this may take a while depending on the number of keywords in your file".format(submission))
        #         for line in kw:
        #             for keyword in line.split():
        #                 #print(keyword)
        #                 results = self.cursor.execute(
        #                     SQL_dict['keyword_match'], (submission, '% ' + keyword + ' %', '% ' + keyword + ' %')).fetchall()
        #                 if results:
        #                     result_string += ' ' + keyword
                
        #             print(result_string)


    def query(self, sql_query: str = 'none'):
        """
        Run custom SQL query.
        :param sql_query: SQL query string
        :return: query results formatted as pandas dataframe
        """
        if sql_query != 'none':
            results = self.cursor.execute(sql_query).fetchall()
            if not results:
                print('Query: {} returned no results'.format(sql_query))
            else:
                return results
        else:
            print("Please enter a valid query. Run 'query --help' for more info.")


    def srx_sa_lcp(self, srx, sa_lcp: str = 'sa_lcp'):
        """
        Extracts study abstract and/or library construction protocol data for an SRA experiment or list of \n
        experiments.
        :param srx: SRX, list of SRXs, or path to a file containing list of SRXs
        :param sa_lcp: OPTIONAL: enter 'sa' for study abstract or 'lcp' for library construction protocol. \n
        By default returns both study abstract and library construction protocol data.
        :return: study abstract and/or library construction protocol data
        """

        results_final = []
        srx_list = []
        switcher = {
            'sa': 'study_abstract',
            'lcp': 'library_construction_protocol',
            'sa_lcp': 'study_abstract, library_construction_protocol'
        }

        if os.path.isfile(str(srx)):
            with open(srx, 'r') as f:
                for line in f:
                    for accession in line.split(','):
                        srx_list.append(accession.rstrip("\n"))
        else:
            if isinstance(srx, tuple):
                srx_list = list(srx)
            else:
                srx_list.append(srx)

        columns = switcher.get(sa_lcp, 'study_abstract, library_construction_protocol')
        query = SQL_dict['srx_sa_lcp'].format(columns)

        for experiment in srx_list:
            results = self.cursor.execute(query, (experiment,)).fetchall()
            for r in results:
                if sa_lcp == 'sa' or sa_lcp == 'lcp':
                    results_final.append(r[0])
                else:
                    results_final.append('abstract:\n' + str(r[0]) + '\n\nlibrary construction protocol:\n' + str(r[1]))

        if not results_final:
            print('Experiment {} contains no study abstract and/or library construction protocol data'.format(srx))

        for result in results_final:
            print(result + '\n')


    def table_info(self, command: str = 'list_all'):
        """
        Return information about database tables. If no param, function returns all tables
        :param command: enter 'table_name' for specific table info
        :return: table info
        """
        if command != 'list_all':
            query_string = 'pragma table_info('+command+');'
            results = self.cursor.execute(query_string).fetchall()
        else:
            print('All tables in SRAdb:')
            results = self.cursor.execute(SQL_dict['list_tables']).fetchall()

        return results


    def terms(self, terms, output: str = 'run_accession', save = False, print_out = True):
        """
        Search for submissions in the metadb that contain ALL provided terms. Run 'cli.py terms -h' for documentation \n
        The experiment columns searched are 'title', 'study_name', 'design_description', \n
        'sample_name', 'library_strategy', 'library_construction_protocol', 'platform', \n
        'instrument_model', and 'platform_parameters'. \n
        The study column searched is 'study_abstract'.
        :param terms: term(s) to search for separated by commas. ex: 'NA12878, Illumina platform, \n
        reagent'. Alternatively, enter the path to a text file of term groups to search for.
        :param output: OPTIONAL: by default run_accessions are outputted. Enter 'experiment_accession', \n
        'run_accession', and/or 'study_accession' if desired. If multiple, separate by commas. Example: \n
        'experiment_accession, run_accession'.
        :param save: OPTIONAL: pass argument 'True' to save accessions to a temporary 'terms' table.
        :param print_out: internal use only.
        :return: run or both study and run accession numbers for entries containing the terms
        """

        if os.path.isfile(str(terms)):
            with open(terms, 'r') as f:
                for line in f:
                    terms_list = []
                    for keyword in line.split(','):
                        terms_list.append(keyword.rstrip("\n"))
                    self._terms_helper(terms_list, output, save, print_out)
        else:
            if isinstance(terms, tuple):
                terms = list(terms)

            return self._terms_helper(terms, output, save, print_out)


    def _terms_helper(self, terms, output: str = 'run_accession', save = False, print_out = True):
        """
        Terms helper function. Method name is preceded by underscore to hide from user.
        """
        
        columns = ['experiment_title', 'study_name', 'design_description', 'sample_name', 'library_strategy', 
                   'library_construction_protocol', 'platform', 'instrument_model', 'platform_parameters', 'study_abstract']

        # if output == 'srr':
        #     query_string = 'SELECT DISTINCT run_accession FROM sra WHERE ('
        # else:
        #     query_string = 'SELECT DISTINCT study_accession, run_accession FROM sra WHERE ('

        query_string = 'SELECT DISTINCT ' + output + ' FROM sra WHERE ('
        output_count = output.count(',') + 1

        if output_count > 3: 
            print('Error: limit output to experiment_accession, run_accession, and/or study_accession only')
            return

        for t in terms:
            for c in columns:
                query_string += c + ' LIKE "%' + t + '%" OR '
            query_string = query_string[:-4]
            query_string += ') AND ('

        query_string = query_string[:-6]

        results = self.cursor.execute(query_string).fetchall()
        if not results:
            print('No submissions match all of the provided terms: {}'.format(terms))
        else:
            if print_out:
                for r in results:
                    #results is a list of tuples
                    if output_count == 1:
                        print(r[0])
                    elif output_count == 2:
                        print(r[0] + ', ' + r[1])
                    else:
                        print(r[0] + ', ' + r[1] + ', ' + r[2])
            else:
                return results

    # def test(self, terms):
    #     terms = list(terms)
    #     print(terms)

if __name__ == "__main__":
    fire.Fire(SRAMetadataX)
