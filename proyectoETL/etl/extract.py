
import pandas as pd


class Extract():
    def __init__(self) -> None:
        self.process = 'Extract Process'

    def read_csv(self, path_file, delimiter, name_headers, headers=None):
       
        df = pd.read_csv(path_file,sep=delimiter,names=name_headers, header=headers)

        df.reset_index(inplace=False)
        
        return df

    def read_tsv(self, path_file, name_headers, headers=None):

        df = pd.read_csv(path_file,sep='\t', header=headers, names=name_headers)

        df.reset_index(inplace=False)
        
        return df

    def read_xml(self, path_file):

        df = pd.read_xml(path_file)
        df.reset_index(inplace=False)
        
        return df

    def read_json(self, path_file):

        df = pd.read_json(path_file)
        df.reset_index(inplace=False)

        return df

