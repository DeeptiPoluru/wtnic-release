import os
import re
import lxml
import shutil
from bs4 import BeautifulSoup
from lxml.html.clean import Cleaner
import urltools


class Utility:

    def __init__(self):
        self.invalid_encodings = [None, '8859_1']
        self.WORDMATCH = re.compile(r'[a-zA-Z]{3,}')
        self.append_issues = dict()
        self.cleaner = Cleaner(
            scripts=True,
            embedded=True,
            meta=True,
            page_structure=True,
            links=True,
            style=True,
            kill_tags=['script', 'style']
        )

        return

    def clean_directory(self):
        shutil.rmtree('data')
        
        if not os.path.exists("data"):
            os.makedirs("data")
        
        return

    def normalize_url(self, web, sub_path=False):
        if web.startswith("http://") or web.startswith("https://"):
            parse = urltools.parse(web)
        else:
            web = "http://" + web
            parse = urltools.parse(web)
        url = parse.domain + "." + parse.tld
        return (url, parse.path) if sub_path else url

    def save_data(self, record_date, company_url, text):
        final_path = ""
        try:
            directory = "/dartfs-hpc/rc/lab/P/PhillipsG/s3data/"
            directory = "arc/"
            normalized_url, sub_path = self.normalize_url(company_url, True)
            final_path += directory + normalized_url
            year = record_date[:4]
            final_path += '/' + year
            level = len(sub_path.split('/')) - 1
            final_path += '/' + str(level) + ".txt"

            if not os.path.exists(os.path.dirname(final_path)):
                os.makedirs(os.path.dirname(final_path))
                append_write = 'w'
            else:
                append_write = 'a'

            data_file = open(final_path, append_write)
            data_file.write(text)
            data_file.write("__info__")
            data_file.close()

        except RuntimeError, ex:
            if final_path in self.append_issues:
                self.append_issues[final_path] += text
            else:
                self.append_issues[final_path] = text
                print ex
            return

        return

    def extract_text_from_html(self, html_data):
        try:
            if isinstance(html_data, bytes):
                if html_data.startswith(b'<?'):
                    html_data = re.sub(b'^\<\?.*?\?\>', b'', html_data, flags=re.DOTALL)
                else:
                    if html_data.startswith('<?'):
                        html_data = re.sub(r'^\<\?.*?\?\>', '', html_data, flags=re.DOTALL)

            clean_text = self.cleaner.clean_html(html_data)
            html_document = lxml.html.document_fromstring(clean_text)
            extracted_text = html_document.text_content()

            words = self.WORDMATCH.findall(extracted_text)
            extracted_text = ' '.join(words)
        
        except Exception as e:
            soup = BeautifulSoup(html_data, 'lxml')
            for script in soup(["script", "style"]):
                script.extract()
            extracted_text = soup.get_text()
            words = self.WORDMATCH.findall(extracted_text)
            extracted_text = ' '.join(words)

            return extracted_text

        return extracted_text

    def resolve_append_issues(self):
        if len(self.append_issues) > 1:
            f = open("append_issues.txt", "w")
            f.write(repr(self.append_issues))
            f.close()

    @staticmethod
    def check_digest(file_name):
        visited_urls = {}
        digest_file_name = file_name.replace(".gz", '.txt')

        if not os.path.exists("digest/" + digest_file_name):
            mode = 'w'
        else:
            mode = 'a'
            with open("digest/" + digest_file_name, "rb") as file_object:
                for url in file_object:
                    visited_urls[url.strip()] = True

        return visited_urls, mode

    @staticmethod
    def save_stats(file_type, file_name, parsing_time, records_count):
        mode = 'a' if os.path.exists("stats.txt") else 'w'
        stats_file = open("stats.txt", mode)
        stats_file.write("Parsed file type : " + file_type + "\n")
        stats_file.write("Parsed file name : " + file_name + "\n")
        stats_file.write("No of records found : " + str(records_count) + "\n")
        stats_file.write("Parsing time: " + str(parsing_time) + "\n")
        stats_file.close()

        return



