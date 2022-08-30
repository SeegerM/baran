########################################
# Dataset
# Mohammad Mahdavi
# moh.mahdavi.l@gmail.com
# October 2017
# Big Data Management Group
# TU Berlin
# All Rights Reserved
########################################


########################################
import re
import sys
import html

import pandas
########################################


########################################


class Dataset:
    """
    The dataset class.
    """

    def __init__(self, dataset_dictionary, n_rows=None):
        """
        The constructor creates a dataset.

        If n_rows is specified, the dataset gets subsetted to the first
        n_rows rows.
        """
        self.name = dataset_dictionary["name"]
        self.path = dataset_dictionary["path"]
        self.dataframe = self.read_csv_dataset(dataset_dictionary["path"])
        if n_rows is not None:
            self.dataframe = self.dataframe.iloc[:n_rows, :]
        if "clean_path" in dataset_dictionary:
            self.has_ground_truth = True
            self.clean_path = dataset_dictionary["clean_path"]
            self.clean_dataframe = self.read_csv_dataset(dataset_dictionary["clean_path"])
            #self.clean_dataframe, self.dataframe = self.checkTokens(self.clean_dataframe, self.dataframe)
            if n_rows is not None:
                self.clean_dataframe = self.clean_dataframe.iloc[:n_rows, :]
        if "repaired_path" in dataset_dictionary:
            self.has_been_repaired = True
            self.repaired_path = dataset_dictionary["repaired_path"]
            self.repaired_dataframe = self.read_csv_dataset(dataset_dictionary["repaired_path"])
            if n_rows is not None:
                self.repaired_dataframe = self.repaired_dataframe.iloc[:n_rows, :]

    def checkTokens(self, dataframe, dirty):
        old_dataframe = dataframe.copy()
        old_dirty_dataframe = dirty.copy()
        for col in dataframe:
            col_split_values = {}
            chars = ["-", " ", "_"]
            for c in chars:
                col_split_values[c] = {}
            for i, row_value in dataframe[col].iteritems():
                for c in chars:
                    split_value = len(str(row_value).split(c))
                    if split_value in col_split_values[c]:
                        col_split_values[c][split_value] = col_split_values[c][split_value] + 1
                    else:
                        col_split_values[c][split_value] = 1

            for c in chars:
                if len(col_split_values[c]) == 1:
                    first_e = list(col_split_values[c].keys())[0]
                    if first_e > 1:
                        print("Normalizing", col)
                        new_cols = dataframe[col].str.split(c, first_e, expand=True)
                        new_cols = new_cols.add_prefix(col)
                        new_cols.fillna("", inplace=True)
                        col_length = len(new_cols.columns)
                        old_dataframe = old_dataframe.join(new_cols)

                        new_cols = old_dirty_dataframe[col].str.split(c, first_e, expand=True)
                        new_cols = new_cols.add_prefix(col)
                        new_cols.fillna("", inplace=True)
                        if col_length != len(new_cols.columns):
                            del new_cols[list(new_cols.columns)[-1]]
                        old_dirty_dataframe = old_dirty_dataframe.join(new_cols)

                        del old_dirty_dataframe[col]
                        del old_dataframe[col]
        cols = ['id', 'article_title', 'article_language', 'article_jvolumn', 'article_jissue', 'article_jcreated_at', 'author_list', 'article_pagination']
        cleaned_cols = ['journal_title', 'journal_issn', 'jounral_abbreviation']
        for col in cleaned_cols:
            for v in old_dataframe[col]:
                x = 1
        df_copy_clean = old_dataframe[cleaned_cols].copy()
        df_copy = old_dirty_dataframe[cols].copy()
        df_copy = df_copy.join(df_copy_clean)
        old_dirty_dataframe = df_copy

        df_copy_clean = old_dataframe[cleaned_cols].copy()
        df_copy = old_dataframe[cols].copy()
        df_copy = df_copy.join(df_copy_clean)
        old_dataframe = df_copy
        return old_dataframe, old_dirty_dataframe


    @staticmethod
    def value_normalizer(value):
        """
        This method takes a value and minimally normalizes it.
        """
        value = html.unescape(value)
        value = re.sub("[\t\n ]+", " ", value, re.UNICODE)
        value = value.strip("\t\n ")
        return value

    def read_csv_dataset(self, dataset_path):
        """
        This method reads a dataset from a csv file path.
        """
        dataframe = pandas.read_csv(dataset_path, sep=",", header="infer", encoding="utf-8", dtype=str,
                                    keep_default_na=False, low_memory=False).applymap(self.value_normalizer)
        return dataframe

    @staticmethod
    def write_csv_dataset(dataset_path, dataframe):
        """
        This method writes a dataset to a csv file path.
        """
        dataframe.to_csv(dataset_path, sep=",", header=True, index=False, encoding="utf-8")

    @staticmethod
    def get_dataframes_difference(dataframe_1, dataframe_2):
        """
        This method compares two dataframes and returns the different cells.
        """
        if dataframe_1.shape != dataframe_2.shape:
            sys.stderr.write("Two compared datasets do not have equal sizes!\n")
        difference_dictionary = {}
        difference_dataframe = dataframe_1.where(dataframe_1.values != dataframe_2.values).notna()
        for j in range(dataframe_1.shape[1]):
            for i in difference_dataframe.index[difference_dataframe.iloc[:, j]].tolist():
                difference_dictionary[(i, j)] = dataframe_2.iloc[i, j]
        return difference_dictionary

    def create_repaired_dataset(self, correction_dictionary):
        """
        This method takes the dictionary of corrected values and creates the repaired dataset.
        """
        self.repaired_dataframe = self.dataframe.copy()
        for cell in correction_dictionary:
            self.repaired_dataframe.iloc[cell] = self.value_normalizer(correction_dictionary[cell])

    def get_df_from_labeled_tuples(self):
        """
        Added by Philipp. Turns the labeled tuples into a dataframe.
        """
        return self.clean_dataframe.iloc[list(self.labeled_tuples.keys()), :]

    def get_actual_errors_dictionary(self):
        """
        This method compares the clean and dirty versions of a dataset.
        """
        return self.get_dataframes_difference(self.dataframe, self.clean_dataframe)

    def get_errors(self):
        """
        This method compares the clean and repaired versions of a dataset.
        """
        return self.get_dataframes_difference(self.repaired_dataframe, self.clean_dataframe)

    def get_correction_dictionary(self):
        """
        This method compares the repaired and dirty versions of a dataset.
        """
        return self.get_dataframes_difference(self.dataframe, self.repaired_dataframe)

    def get_data_quality(self):
        """
        This method calculates data quality of a dataset.
        """
        return 1.0 - float(len(self.get_actual_errors_dictionary())) / (self.dataframe.shape[0] * self.dataframe.shape[1])

    def get_data_cleaning_evaluation(self, correction_dictionary, dirty, sampled_rows_dictionary=False):
        """
        This method evaluates data cleaning process.
        """

        ec_tp_feature = 0.0
        ec_tp_feature_standalone = 0.0
        actual_errors = self.get_actual_errors_dictionary()
        #feature = Feature(dirty)

        if sampled_rows_dictionary:
            actual_errors = {(i, j): actual_errors[(i, j)] for (i, j) in actual_errors if i in sampled_rows_dictionary}
        ed_tp = 0.0
        ec_tp = 0.0
        output_size = 0.0
        for cell in correction_dictionary:
            if (not sampled_rows_dictionary) or (cell[0] in sampled_rows_dictionary):
                output_size += 1
                if cell in actual_errors:
                    ed_tp += 1.0
                    if correction_dictionary[cell] == actual_errors[cell]:
                        ec_tp += 1.0
                        ec_tp_feature += 1.0

                    #else:
                        #print("Correctur: ", dirty.iloc[cell[0]][dirty.columns[cell[1]]], " -> ", correction_dictionary[cell], "should be:", actual_errors[cell])
                        #if feature.checkForImprovement(self.clean_dataframe, dirty, correction_dictionary[cell], actual_errors[cell], cell):
                        #    ec_tp_feature += 1.0
                        #else:
                            #print("No result for: ",  correction_dictionary[cell], actual_errors[cell], " in ", dirty.columns[cell[1]], " Old Value ", dirty.iloc[cell[0]][dirty.columns[cell[1]]])
       #feature.printImrpovement(get_prf(output_size, ec_tp_feature, actual_errors))
        ed_p = 0.0 if output_size == 0 else ed_tp / output_size
        ed_r = 0.0 if len(actual_errors) == 0 else ed_tp / len(actual_errors)
        ed_f = 0.0 if (ed_p + ed_r) == 0.0 else (2 * ed_p * ed_r) / (ed_p + ed_r)

        ec_p = 0.0 if output_size == 0 else ec_tp / output_size
        ec_r = 0.0 if len(actual_errors) == 0 else ec_tp / len(actual_errors)
        ec_f = 0.0 if (ec_p + ec_r) == 0.0 else (2 * ec_p * ec_r) / (ec_p + ec_r)
        print("All errors:", len(actual_errors), "True positive", ec_tp, "Positives", output_size)
        return [ed_p, ed_r, ed_f, ec_p, ec_r, ec_f]

def get_prf(output_size, ec_tp, actual_errors):
    p = 0.0 if output_size == 0 else ec_tp / output_size
    r = 0.0 if len(actual_errors) == 0 else ec_tp / len(actual_errors)
    f = 0.0 if (p + r) == 0.0 else (2 * p * r) / (p + r)
    return [p, r, f]

########################################


########################################
if __name__ == "__main__":
    dataset_dict = {
        "name": "toy",
        "path": "datasets/dirty.csv",
        "clean_path": "datasets/clean.csv"
    }
    d = Dataset(dataset_dict)
    print(d.get_data_quality())
########################################
