import fire
import sqlite3
import pandas as pd
import os
import requests
import sys
import gzip
import shutil
from tqdm.autonotebook import tqdm

SQLITE_URL = [
    "https://s3.amazonaws.com/starbuck1/sradb/SRAmetadb.sqlite.gz",
    "https://gbnci-abcc.ncifcrf.gov/backup/SRAmetadb.sqlite.gz",
]

SQL_dict = {'list_tables': 'SELECT name FROM sqlite_master WHERE type="table";',
            'count_lcp': 'SELECT count(library_construction_protocol) FROM experiment WHERE library_construction_protocol like ? OR library_construction_protocol like ?;',
            'all_acc_lcp': 'SELECT submission_accession FROM experiment WHERE library_construction_protocol like ? OR library_construction_protocol like ?;',
            'all_acc_sm': 'SELECT submission_accession FROM sample WHERE description!=?;',
            'keyword_match': 'SELECT DISTINCT run_accession FROM sra WHERE library_construction_protocol LIKE ? OR study_abstract LIKE ?',
            'sra_lcp': 'SELECT library_construction_protocol FROM experiment WHERE submission_accession=?',
            'sra_sm': 'SELECT description FROM sample WHERE submission_accession=?'}


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
        self.sqlite_file = os.path.join(os.getcwd(), "SRAmetadb.sqlite")
        try:
            self.db = sqlite3.connect(
                "file:{}?mode=rw".format(self.sqlite_file), uri=True)
        except:
            value = input(
                "SRAmetadb sqlite file not found in current directory. Download file? Enter [y/n]:\n")
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

        self.cursor = self.db.cursor()

    def all_lcp(self):
        """
        List all SRA submissions that contain library construction protocol data
        :return: submission accession numbers
        """
        results = self.cursor.execute(
            SQL_dict['all_acc_lcp'], ('% kit %', '% reagent %')).fetchall()
        return results

    def _download(self, url, file_path):
        """
        Download helper function. Method name is preceded by underscore to hide from user.
        """
        with open(file_path, "wb") as f:
            print("Downloading {}".format(file_path))
            #response = requests.get(url, stream=True)
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
            self._download(SQLITE_URL[0], dl_path)
        except Exception as e:
            # Try NCBI
            sys.stderr.write(
                "Could not use AWS s3 {}.\nException: {}.\nTrying NCBI...\n".format(
                    SQLITE_URL[0], e
                )
            )
            try:
                self._download(SQLITE_URL[1], dl_path)
            except Exception as e:
                sys.stderr.write(
                    "Could not use NCBI {}.\nException: {}.\nPlease download the SQlite file via wget...\n".format(
                        SQLITE_URL[0], e
                    )
                )

        print("Extracting {} ...".format(dl_path))

        with gzip.open(dl_path, "rb") as f_in:
            with open(dl_path_unzipped, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        print("SRAmetadb file download complete!")
        metadata = self.query("SELECT * FROM metaInfo")
        print("SRAdb file Metadata:")
        print(metadata)

    def keyword_match(self, submissions, keyword_file, save: str = 'true'):
        """
        Search a given list of submissions for matching keywords. 
        Stores keywords and associated SRRs in the variables table.
        :param submissions: user defined file of submissions. Use terms function and output to this file.
        :param keyword_file: user defined file of keywords
        :param save: OPTIONAL: by default stores keywords and associated SRRs in database \n
        in a table called terms. Enter 'ns' if you do not wish to store keywords.
        :return: keywords and their associated SRRs
        """
        with open(keyword_file, 'r') as f:
            sys.stdout.write(
                "Reading {}.. this may take a while depending on the number of keywords in your file".format(keyword_file))
            for line in f:
                for keyword in line.split():
                    print(keyword)
                    results = self.cursor.execute(
                        SQL_dict['keyword_match'], ('% ' + keyword + ' %', '% ' + keyword + ' %')).fetchall()
                    for r in results:
                        for tup in r:
                            print(tup)


    def query(self, sql_query: str = 'oogabooga'):
        """
        Run custom SQL query.
        :param sql_query: SQL query string
        :return: query results formatted as pandas dataframe
        """
        results = self.cursor.execute(sql_query).fetchall()
        return results

    def sra_lcp(self, sra: str = 'oogityboogity'):
        """
        Extracts library construction protocol data for an SRA submission
        :param sra: SRA identifier
        :return: library construction protocol data
        """
        #results = self.cursor.execute(SQL_dict['all_acc_rk'], ('% kit %', '% reagent %')).fetchone()
        results = self.cursor.execute(SQL_dict['sra_lcp'], (sra,)).fetchall()
        return results

    def sra_sm(self, sra: str = 'oogabooga'):
        """
        Extracts sample manipulation data for an SRA submission
        :param sra: SRA identifier
        :return: sample manipulation data
        """
        #results = self.cursor.execute(SQL_dict['all_acc_sm'], ('null',)).fetchone()
        results = self.cursor.execute(SQL_dict['sra_sm'], (sra,)).fetchall()
        return results

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

    def terms(self, terms, output: str = 'srr', save: str = 'true'):
        """
        Search for submissions in the metadb that contain ALL provided terms. Run 'cli.py terms -h' for documentation \n
        The experiment columns searched are 'title', 'study_name', 'design_description', \n
        'sample_name', 'library_strategy', 'library_construction_protocol', 'platform', \n
        'instrument_model', and 'platform_parameters'. \n
        The study column searched is 'study_abstract'.
        :param terms: term(s) to search for separated by commas. ex: 'NA12878, Illumina platform, \n
        reagent'. Alternatively, enter the path to a text file of term groups to search for.
        :param output: OPTIONAL: by default SRRs are outputted. Enter 'srp_srr' if \n
        you want both srp (study) and srr (run) accessions.
        :return: submission and run accession numbers for submissions containing the terms
        """

        if os.path.isfile(str(terms)):
            with open(terms, 'r') as f:
                for line in f:
                    terms_list = []
                    for keyword in line.split(','):
                        terms_list.append(keyword.rstrip("\n"))
                    self._terms_helper(terms_list, save, output)
        else:
            try:
                terms = terms.split(',')
            except:
                pass

            self._terms_helper(terms, save, output)


    def _terms_helper(self, terms, save: str = 'true', output: str = 'srr'):
        """
        Terms helper function. Method name is preceded by underscore to hide from user.
        """
        
        columns = ['experiment_title', 'study_name', 'design_description', 'sample_name', 'library_strategy', 'library_construction_protocol',
                   'platform', 'instrument_model', 'platform_parameters', 'study_abstract']

        if output == 'srr':
            query_string = 'SELECT DISTINCT run_accession FROM sra WHERE ('
        else:
            query_string = 'SELECT DISTINCT study_accession, run_accession FROM sra WHERE ('

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
            for r in results:
                #results is a list of tuples
                if output == 'srr':
                    print(r[0])
                else:
                    print(r[0] + ', ' + r[1])


if __name__ == "__main__":
    fire.Fire(SRAMetadataX)
