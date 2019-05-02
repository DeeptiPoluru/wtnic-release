import os
import sys
import time
import multiprocessing
from utility import Utility
from warcio.archiveiterator import ArchiveIterator
import urltools
import logging


class Parser:

    def __init__(self):
        reload(sys)
        self.records_count = 0
        self.utility = Utility()
        self.error_log = self.setup_logger("error_log", "error_logs.log")
        self.parsing_log = self.setup_logger("parsing_log", "parsing_logs.log")

        if not os.path.exists("digest"):
            os.makedirs("digest")

        return

    @staticmethod
    def setup_logger(logger_name, log_file, level=logging.INFO):
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger

    def process_company_data(self, company, company_pages):
        for page in company_pages:
            content, record_date, company_url = page
            text = self.utility.extract_text_from_html(content)
            self.utility.save_data(record_date, company_url, text)

    def create_processes(self, company_dict):
        jobs = []
        for company, data in company_dict.iteritems():
            p = multiprocessing.Process(target=self.process_company_data, args=(company, data))
            jobs.append(p)

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()

    def normalize_url(self, web, sub_url=False):
        if web.startswith("http://") or web.startswith("https://"):
            parse = urltools.parse(web)
        else:
            web = "http://" + web
            parse = urltools.parse(web)
        url = parse.domain + "." + parse.tld
        return (url, parse.path) if sub_url else url

    def warc_file_parser(self, file_name, filepath):
        try:
            self.records_count = 0
            start_time = time.time()
            self.parsing_log.info("Parsing a warc file: " + file_name)
            visited_urls, mode = self.utility.check_digest(file_name)
            digest_file_name = file_name.replace(".gz", '.txt')
            digest_file = open("digest/" + digest_file_name, mode)

            multiprocess_data_dict = {}

            with open(filepath, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    try:
                        if record.rec_type == 'response':
                            self.records_count += 1
                            record_date = record.rec_headers.get_header('WARC-Date')
                            company_url = record.rec_headers.get_header('WARC-Target-URI')
                            normalized_url = self.normalize_url(company_url)
                            if company_url not in visited_urls:
                                s = record.content_stream().read()
                                if normalized_url not in multiprocess_data_dict:
                                    if len(multiprocess_data_dict) < 16:
                                        multiprocess_data_dict[normalized_url] = [(s, record_date, company_url)]
                                    else:
                                        self.create_processes(multiprocess_data_dict)
                                        multiprocess_data_dict = {normalized_url: [(s, record_date, company_url)]}
                                else:
                                    if len(multiprocess_data_dict[normalized_url]) >= 500:
                                        self.create_processes(multiprocess_data_dict)
                                        multiprocess_data_dict = {normalized_url: [(s, record_date, company_url)]}
                                    else:
                                        multiprocess_data_dict[normalized_url].append((s, record_date, company_url))

                                digest_file.write(company_url)
                                digest_file.write("\n")

                    except RuntimeError, ex:
                        self.error_log.exception(ex)
                self.create_processes(multiprocess_data_dict)
                digest_file.close()
                parsing_time = time.time() - start_time
                self.utility.save_stats('warcs', file_name, parsing_time, self.records_count)

        except IOError, ex:
            self.error_log.exception(ex)

        return

    def arc_file_parser(self, file_name, filepath):
        try:
            start_time = time.time()
            self.records_count = 0
            self.parsing_log.info("Parsing an arc file: " + file_name)
            visited_urls, mode = self.utility.check_digest(file_name)
            digest_file_name = file_name.replace(".gz", '.txt')
            digest_file = open("digest/" + digest_file_name, mode)

            multiprocess_data_dict = {}

            with open(filepath, 'rb') as stream:
                for record in ArchiveIterator(stream, arc2warc=True):
                    try:
                        if record.rec_type == 'response':
                            self.records_count += 1
                            record_date = record.rec_headers.get_header('WARC-Date')
                            company_url = record.rec_headers.get_header('WARC-Target-URI')
                            normalized_url = self.normalize_url(company_url)

                            if company_url not in visited_urls:
                                s = record.content_stream().read()
                                if normalized_url not in multiprocess_data_dict:
                                    if len(multiprocess_data_dict) < 16:
                                        multiprocess_data_dict[normalized_url] = [(s, record_date, company_url)]
                                    else:
                                        self.create_processes(multiprocess_data_dict)
                                        multiprocess_data_dict = {normalized_url: [(s, record_date, company_url)]}
                                else:
                                    if len(multiprocess_data_dict[normalized_url]) >= 500:
                                        self.create_processes(multiprocess_data_dict)
                                        multiprocess_data_dict = {normalized_url: [(s, record_date, company_url)]}
                                    else:
                                        multiprocess_data_dict[normalized_url].append((s, record_date, company_url))
                                
                                digest_file.write(company_url)
                                digest_file.write("\n")

                    except Exception, ex:
                        self.error_log.exception(file_name)
                        pass
                self.create_processes(multiprocess_data_dict)
                digest_file.close()
                parsing_time = time.time() - start_time
                self.utility.save_stats('arcs', file_name, parsing_time, self.records_count)

        except Exception, ex:
            self.error_log.exception(file_name)
            pass

        return

    def parse_file(self):
        try:
            with open("md5.txt", "r") as file_object:
                for line in file_object:
                    line = line.strip().split()
                    file_name = line[1].strip()
                    filepath = os.path.join(line[2].strip(), file_name)
                    if not os.path.isfile(filepath):
                        self.error_log.error("MD5_FILE_NOT_FOUND," + filepath)
                        continue
                    digest_file_name = file_name.replace(".gz", '.txt')
                    if os.path.isfile("digest/" + digest_file_name):
                        print "{} exists in digest already, ignore it.".format(file_name)
                        continue

                    if ".arc.gz" in file_name:
                        self.records_count = 0
                        self.arc_file_parser(file_name, filepath)

                    elif ".warc.gz" in file_name:
                        self.records_count = 0
                        self.warc_file_parser(file_name, filepath)

        except IOError, ex:
            self.error_log.exception(ex)

        return


if __name__ == "__main__":
    parser = Parser()
    parser.parse_file()
    parser.utility.resolve_append_issues()
