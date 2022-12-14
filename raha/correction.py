########################################
# Baran: The Error Correction System
# Mohammad Mahdavi
# moh.mahdavi.l@gmail.com
# April 2019
# Big Data Management Group
# TU Berlin
# All Rights Reserved
########################################


########################################
import os
import io
import sys
import math
import json
import pickle
import difflib
import unicodedata
import multiprocessing
import re
import nltk
import time

import bs4
import bz2
import numpy
import py7zr
import mwparserfromhell
import sklearn.svm
import sklearn.ensemble
import sklearn.naive_bayes
import sklearn.linear_model
from typing import Dict
import pandas as pd
from collections import defaultdict

from IPython.core.debugger import set_trace

import datawig
import raha
from raha import imputer
from raha import pdep
########################################


########################################
from raha.customfeature import Feature
from raha.structs import MultiLSH


class Correction:
    """
    The main class.
    """

    def __init__(self):
        """
        The constructor.
        """
        self.PRETRAINED_VALUE_BASED_MODELS_PATH = ""
        self.VALUE_ENCODINGS = ["identity", "unicode"]
        self.CLASSIFICATION_MODEL = "ABC"  # ["ABC", "DTC", "GBC", "GNB", "KNC" ,"SGDC", "SVC"]
        self.IGNORE_SIGN = "<<<IGNORE_THIS_VALUE>>>"
        self.VERBOSE = False
        self.SAVE_RESULTS = True
        self.ONLINE_PHASE = False
        self.LABELING_BUDGET = 20
        self.MIN_CORRECTION_CANDIDATE_PROBABILITY = 0.00
        self.MIN_CORRECTION_OCCURRENCE = 2
        self.MAX_VALUE_LENGTH = 50
        self.REVISION_WINDOW_SIZE = 5

        # Philipps changes
        # choose from "value", "domain", "vicinity", "imputer". Default is Baran's original configuration.
        self.FEATURE_GENERATORS = ["value", "domain", "vicinity"]
        self.VICINITY_ORDERS = [1]  # Baran default
        self.VICINITY_FEATURE_GENERATOR = "naive"  # "naive" or "pdep". naive is Baran's original strategy.
        self.IMPUTER_CACHE_MODEL = True  # use cached model if true. train new imputer model otherwise.

        # conditional probability * gpdep_score needs to be higher than this threshold
        # for a correction candidate to get turned in to a feature.
        self.GPDEP_CORRECTION_SCORE_THRESHOLD = 0.05

        # recommend up to 10. Ignored when using 'naive' feature generator. That one
        # always generates features for all possible column combinations.
        self.N_BEST_PDEPS = None
        self.df = pd.DataFrame()

        self.already_verified = {}
        self.feature = None

    @staticmethod
    def _wikitext_segmenter(wikitext):
        """
        This method takes a Wikipedia page revision text in wikitext and segments it recursively.
        """

        def recursive_segmenter(node):
            if isinstance(node, str):
                segments_list.append(node)
            elif isinstance(node, mwparserfromhell.nodes.text.Text):
                segments_list.append(node.value)
            elif not node:
                pass
            elif isinstance(node, mwparserfromhell.wikicode.Wikicode):
                for n in node.nodes:
                    if isinstance(n, str):
                        recursive_segmenter(n)
                    elif isinstance(n, mwparserfromhell.nodes.text.Text):
                        recursive_segmenter(n.value)
                    elif isinstance(n, mwparserfromhell.nodes.heading.Heading):
                        recursive_segmenter(n.title)
                    elif isinstance(n, mwparserfromhell.nodes.tag.Tag):
                        recursive_segmenter(n.contents)
                    elif isinstance(n, mwparserfromhell.nodes.wikilink.Wikilink):
                        if n.text:
                            recursive_segmenter(n.text)
                        else:
                            recursive_segmenter(n.title)
                    elif isinstance(n, mwparserfromhell.nodes.external_link.ExternalLink):
                        # recursive_parser(n.url)
                        recursive_segmenter(n.title)
                    elif isinstance(n, mwparserfromhell.nodes.template.Template):
                        recursive_segmenter(n.name)
                        for p in n.params:
                            # recursive_parser(p.name)
                            recursive_segmenter(p.value)
                    elif isinstance(n, mwparserfromhell.nodes.html_entity.HTMLEntity):
                        segments_list.append(n.normalize())
                    elif not n or isinstance(n, mwparserfromhell.nodes.comment.Comment) or \
                            isinstance(n, mwparserfromhell.nodes.argument.Argument):
                        pass
                    else:
                        sys.stderr.write("Inner layer unknown node found: {}, {}\n".format(type(n), n))
            else:
                sys.stderr.write("Outer layer unknown node found: {}, {}\n".format(type(node), node))

        try:
            parsed_wikitext = mwparserfromhell.parse(wikitext)
        except:
            parsed_wikitext = ""
        segments_list = []
        recursive_segmenter(parsed_wikitext)
        return segments_list

    def extract_revisions(self, wikipedia_dumps_folder):
        """
        This method takes the folder path of Wikipedia page revision history dumps and extracts the value-based corrections.
        """
        rd_folder_path = os.path.join(wikipedia_dumps_folder, "revision-data")
        if not os.path.exists(rd_folder_path):
            os.mkdir(rd_folder_path)
        compressed_dumps_list = [df for df in os.listdir(wikipedia_dumps_folder) if df.endswith(".7z")]
        page_counter = 0
        for file_name in compressed_dumps_list:
            compressed_dump_file_path = os.path.join(wikipedia_dumps_folder, file_name)
            dump_file_name, _ = os.path.splitext(os.path.basename(compressed_dump_file_path))
            rdd_folder_path = os.path.join(rd_folder_path, dump_file_name)
            if not os.path.exists(rdd_folder_path):
                os.mkdir(rdd_folder_path)
            else:
                continue
            archive = py7zr.SevenZipFile(compressed_dump_file_path, mode="r")
            archive.extractall(path=wikipedia_dumps_folder)
            archive.close()
            decompressed_dump_file_path = os.path.join(wikipedia_dumps_folder, dump_file_name)
            decompressed_dump_file = io.open(decompressed_dump_file_path, "r", encoding="utf-8")
            page_text = ""
            for i, line in enumerate(decompressed_dump_file):
                line = line.strip()
                if line == "<page>":
                    page_text = ""
                page_text += "\n" + line
                if line == "</page>":
                    revisions_list = []
                    page_tree = bs4.BeautifulSoup(page_text, "html.parser")
                    previous_text = ""
                    for revision_tag in page_tree.find_all("revision"):
                        revision_text = revision_tag.find("text").text
                        if previous_text:
                            a = [t for t in self._wikitext_segmenter(previous_text) if t]
                            b = [t for t in self._wikitext_segmenter(revision_text) if t]
                            s = difflib.SequenceMatcher(None, a, b)
                            for tag, i1, i2, j1, j2 in s.get_opcodes():
                                if tag == "equal":
                                    continue
                                revisions_list.append({
                                    "old_value": a[i1:i2],
                                    "new_value": b[j1:j2],
                                    "left_context": a[i1 - self.REVISION_WINDOW_SIZE:i1],
                                    "right_context": a[i2:i2 + self.REVISION_WINDOW_SIZE]
                                })
                        previous_text = revision_text
                    if revisions_list:
                        page_counter += 1
                        if self.VERBOSE and page_counter % 100 == 0:
                            for entry in revisions_list:
                                print("----------Page Counter:---------\n", page_counter,
                                      "\n----------Old Value:---------\n", entry["old_value"],
                                      "\n----------New Value:---------\n", entry["new_value"],
                                      "\n----------Left Context:---------\n", entry["left_context"],
                                      "\n----------Right Context:---------\n", entry["right_context"],
                                      "\n==============================")
                        json.dump(revisions_list, open(os.path.join(rdd_folder_path, page_tree.id.text + ".json"), "w"))
            decompressed_dump_file.close()
            os.remove(decompressed_dump_file_path)
            if self.VERBOSE:
                print("{} ({} / {}) is processed.".format(file_name, len(os.listdir(rd_folder_path)),
                                                          len(compressed_dumps_list)))

    @staticmethod
    def _value_encoder(value, encoding):
        """
        This method represents a value with a specified value abstraction encoding method.
        """
        if encoding == "identity":
            return json.dumps(list(value))
        if encoding == "unicode":
            return json.dumps([unicodedata.category(c) for c in value])

    @staticmethod
    def _to_model_adder(model, key, value):
        """
        This methods incrementally adds a key-value into a dictionary-implemented model.
        """
        if key not in model:
            model[key] = {}
        if value not in model[key]:
            model[key][value] = 0.0
        model[key][value] += 1.0

    def getSuggestedTyp(self, dic, value):
        dic = json.loads(dic)
        valueSplit = re.split(r'\D+', value)
        # print(valueSplit)
        if int(dic["rounding"]) == -1:
            if (len(valueSplit) == 1 or valueSplit[1] == "" or float(valueSplit[1]) == 0) and dic["minimal"]:
                return valueSplit[0] + dic["endingTyp"]
            else:
                if len(valueSplit) == 1:
                    return valueSplit[0] + dic["divider"] + dic["endingTyp"]
                else:
                    return valueSplit[0] + dic["divider"] + str(int(valueSplit[1])) + dic["endingTyp"]
        else:
            if (len(valueSplit) == 1 or valueSplit[1] == "" or float(valueSplit[1]) == 0) and dic["minimal"]:
                return valueSplit[0] + dic["divider"] + str(float(0)), dic["endingTyp"]
            else:
                return str(
                    round(float(str(valueSplit[0]) + str(dic["divider"]) + str(valueSplit[1])), dic["rounding"])) + dic[
                           "endingTyp"]

    def _typ_based_models_updater(self, models, ud):
        # print("Type: ", ud["old_value"], ud["new_value"], any(char.isdigit() for char in ud["new_value"]))
        if ud["new_value"].isnumeric() or ud["new_value"].isdigit():
            self.only_nums(models, ud)
        elif self.isfloat(ud["new_value"]):
            # print("Float found")
            self.only_nums(models, ud)
        elif any(char.isdigit() for char in ud["new_value"]):
            valueSplit = re.split(r'\D+', ud["new_value"])
            # print(valueSplit)
        # else:
        # print("String Value")

    def isfloat(self, num):
        try:
            float(num)
            return True
        except ValueError:
            return False

    def only_nums(self, models, ud):
        divider = " "
        if '.' in ud["new_value"]:
            divider = '.'
        elif ',' in ud["new_value"]:
            divider = ','
        elif '.' in ud["old_value"]:
            divider = '.'
        elif ',' in ud["old_value"]:
            divider = ','
        if len(ud["new_value"].split(divider)) == 1:
            rounding = -1
            divider = ""
        else:
            rounding = len(ud["new_value"].split(divider)[1])
        colName = ud["column"]
        endingTyp = ""
        valueSplit = re.split(r'\D+', ud["new_value"])
        if len(valueSplit) > 1:
            minimal = False
        else:
            minimal = True
        listTyp = {
            "divider": divider,
            "rounding": rounding,
            "endingTyp": endingTyp,
            "minimal": minimal,
        }
        models[4][colName] = json.dumps(listTyp)
        # print("TypBased:", str(divider) + " " + str(rounding) + " " + str(colName), ud["new_value"])

    def _value_based_models_updater(self, models, ud):
        """
        This method updates the value-based error corrector models with a given update dictionary.
        """
        # TODO: adding jabeja konannde bakhshahye substring
        if self.ONLINE_PHASE or (ud["new_value"] and len(ud["new_value"]) <= self.MAX_VALUE_LENGTH and
                                 ud["old_value"] and len(ud["old_value"]) <= self.MAX_VALUE_LENGTH and
                                 ud["old_value"] != ud["new_value"] and ud["old_value"].lower() != "n/a" and
                                 not ud["old_value"][0].isdigit()):
            remover_transformation = {}
            adder_transformation = {}
            replacer_transformation = {}
            s = difflib.SequenceMatcher(None, ud["old_value"], ud["new_value"])
            for tag, i1, i2, j1, j2 in s.get_opcodes():
                index_range = json.dumps([i1, i2])
                if tag == "delete":
                    remover_transformation[index_range] = ""
                if tag == "insert":
                    adder_transformation[index_range] = ud["new_value"][j1:j2]
                if tag == "replace":
                    replacer_transformation[index_range] = ud["new_value"][j1:j2]
            for encoding in self.VALUE_ENCODINGS:
                encoded_old_value = self._value_encoder(ud["old_value"], encoding)
                if remover_transformation:
                    self._to_model_adder(models[0], encoded_old_value, json.dumps(remover_transformation))
                    # print("Transformation remove: ", encoded_old_value, remover_transformation)
                if adder_transformation:
                    self._to_model_adder(models[1], encoded_old_value, json.dumps(adder_transformation))
                    # print("Transformation add: ", encoded_old_value, remover_transformation)
                if replacer_transformation:
                    self._to_model_adder(models[2], encoded_old_value, json.dumps(replacer_transformation))
                    # print("Transformation replace: ", encoded_old_value, remover_transformation)
                self._to_model_adder(models[3], encoded_old_value, ud["new_value"])

    def pretrain_value_based_models(self, revision_data_folder):
        """
        This method pretrains value-based error corrector models.
        """

        def _models_pruner():
            for mi, model in enumerate(models):
                for k in list(model.keys()):
                    for v in list(model[k].keys()):
                        if model[k][v] < self.MIN_CORRECTION_OCCURRENCE:
                            models[mi][k].pop(v)
                    if not models[mi][k]:
                        models[mi].pop(k)

        models = [{}, {}, {}, {}]
        rd_folder_path = revision_data_folder
        page_counter = 0
        for folder in os.listdir(rd_folder_path):
            if os.path.isdir(os.path.join(rd_folder_path, folder)):
                for rf in os.listdir(os.path.join(rd_folder_path, folder)):
                    if rf.endswith(".json"):
                        page_counter += 1
                        if page_counter % 100000 == 0:
                            _models_pruner()
                            if self.VERBOSE:
                                print(page_counter, "pages are processed.")
                        try:
                            revision_list = json.load(
                                io.open(os.path.join(rd_folder_path, folder, rf), encoding="utf-8"))
                        except:
                            continue
                        for rd in revision_list:
                            update_dictionary = {
                                "old_value": raha.dataset.Dataset.value_normalizer("".join(rd["old_value"])),
                                "new_value": raha.dataset.Dataset.value_normalizer("".join(rd["new_value"]))
                            }
                            self._value_based_models_updater(models, update_dictionary)
        _models_pruner()
        pretrained_models_path = os.path.join(revision_data_folder, "pretrained_value_based_models.dictionary")
        if self.PRETRAINED_VALUE_BASED_MODELS_PATH:
            pretrained_models_path = self.PRETRAINED_VALUE_BASED_MODELS_PATH
        pickle.dump(models, bz2.BZ2File(pretrained_models_path, "wb"))
        if self.VERBOSE:
            print("The pretrained value-based models are stored in {}.".format(pretrained_models_path))

    def _domain_based_model_updater(self, model, ud):
        """
        This method updates the domain-based error corrector model with a given update dictionary.
        """
        self._to_model_adder(model, ud["column"], ud["new_value"])

    def _value_based_corrector(self, models, ed):
        """
        This method takes the value-based models and an error dictionary to generate potential value-based corrections.
        """
        results_list = []
        results_type_list = []
        for m, model_name in enumerate(["remover", "adder", "replacer", "swapper", "typ"]):
            model = models[m]
            if model_name == "typ":
                results_dictionaryTyp = {}
                if ed["column"] in model:
                    suggestion = self.getSuggestedTyp(model[ed["column"]], ed["old_value"])
                    # print("My Suggestion:", ed["old_value"], results_dictionaryTyp, model)
                    # for index in [0,1,2,3,4,5]:
                    #   results_list[index] = results_dictionaryTyp
                    if len(suggestion) > 0:
                        results_dictionaryTyp[suggestion] = 1.0
                results_type_list.append(results_dictionaryTyp)
                continue
            for encoding in self.VALUE_ENCODINGS:
                results_dictionary = {}
                encoded_value_string = self._value_encoder(ed["old_value"], encoding)
                if encoded_value_string in model:
                    sum_scores = sum(model[encoded_value_string].values())
                    if model_name in ["remover", "adder", "replacer"]:
                        for transformation_string in model[encoded_value_string]:
                            index_character_dictionary = {i: c for i, c in enumerate(ed["old_value"])}
                            transformation = json.loads(transformation_string)
                            for change_range_string in transformation:
                                change_range = json.loads(change_range_string)
                                if model_name in ["remover", "replacer"]:
                                    for i in range(change_range[0], change_range[1]):
                                        index_character_dictionary[i] = ""
                                if model_name in ["adder", "replacer"]:
                                    ov = "" if change_range[0] not in index_character_dictionary else \
                                        index_character_dictionary[change_range[0]]
                                    index_character_dictionary[change_range[0]] = transformation[
                                                                                      change_range_string] + ov
                            new_value = ""
                            for i in range(len(index_character_dictionary)):
                                new_value += index_character_dictionary[i]
                            pr = model[encoded_value_string][transformation_string] / sum_scores
                            if pr >= self.MIN_CORRECTION_CANDIDATE_PROBABILITY:
                                results_dictionary[new_value] = pr
                    if model_name == "swapper":
                        for new_value in model[encoded_value_string]:
                            pr = model[encoded_value_string][new_value] / sum_scores
                            if pr >= self.MIN_CORRECTION_CANDIDATE_PROBABILITY:
                                results_dictionary[new_value] = pr
                results_list.append(results_dictionary)
        return results_list, results_type_list

    def _imputer_based_corrector(self, model: Dict[int, pd.DataFrame], ed: dict) -> list:
        """
        Use an AutoGluon imputer to generate corrections.

        @param model: Dictionary with an AutoGluonImputer per column.
        @param ed: Error Dictionary with information on the error and its vicinity.
        @return: list of corrections.
        """
        df_probas = model.get(ed['column'])
        if df_probas is None:
            return []
        probas = df_probas.iloc[ed['row']]

        prob_d = {key: probas.to_dict()[key] for key in probas.to_dict()}
        prob_d_sorted = {key: value for key, value in sorted(prob_d.items(), key=lambda x: x[1])}

        result = {}
        for correction, probability in prob_d_sorted.items():
            # make sure that suggested correction is likely and isn't the old error
            if probability > 0 and correction != ed['old_value']:
                result[correction] = probability
        # TODO normalize probabilities when old error gets deleted
        return [result]

    def _domain_based_corrector(self, model, ed):
        """
        This method takes a domain-based model and an error dictionary to generate potential domain-based corrections.
        """
        results_dictionary = {}
        value_counts = model.get(ed["column"])
        if value_counts is not None:
            sum_scores = sum(model[ed["column"]].values())
            for new_value in model[ed["column"]]:
                pr = model[ed["column"]][new_value] / sum_scores
                if pr >= self.MIN_CORRECTION_CANDIDATE_PROBABILITY:
                    results_dictionary[new_value] = pr
        return [results_dictionary]

    def _domain_based_corrector_sim_3(self, value, col):
        results_dictionary = {}
        lsh = MultiLSH()
        for x in col:
            lsh.add(x)
        min_value, sim = lsh.sim(value)
        results_dictionary[min_value] = sim
        return [results_dictionary]

    def _domain_based_corrector_sim_2(self, value, lsh):
        results_dictionary = {}
        min_value, sim = lsh.sim(value)
        if min_value is not None:
            results_dictionary[min_value] = sim
        return [results_dictionary]

    def _domain_based_corrector_sim(self, value, col):
        results_dictionary = {}
        minValue = ""
        minDis = 10000000000.0
        # if value in col:
        #    return [results_dictionary]
        value_tokens = self.shingle(value, 2)
        if len(value_tokens) == 0:
            return [results_dictionary]
        for x in set(col):
            if x == self.IGNORE_SIGN:
                continue
            dist = nltk.edit_distance(value, x)
            # dist = self.jaccard_similarity(value_tokens, self.shingle(x, 2))
            if dist < minDis:
                minValue = x
                minDis = dist
        # print(value, minValue)
        results_dictionary[minValue] = 0.99
        return [results_dictionary]

    def shingle(self, text: str, k: int):
        shingle_set = []
        for i in range(len(text) - k + 1):
            shingle_set.append(text[i:i + k])
        return set(shingle_set)

    def jaccard_similarity(self, list1, list2):
        intersection = len(list(set(list1).intersection(list2)))
        union = (len(set(list1)) + len(set(list2))) - intersection
        return float(intersection) / union

    def initialize_dataset(self, d):
        """
        This method initializes the dataset.
        """
        self.ONLINE_PHASE = True
        d.results_folder = os.path.join(os.path.dirname(d.path), "raha-baran-results-" + d.name)
        if self.SAVE_RESULTS and not os.path.exists(d.results_folder):
            os.mkdir(d.results_folder)
        d.column_errors = {}
        for cell in d.detected_cells:
            self._to_model_adder(d.column_errors, cell[1], cell)
        d.labeled_tuples = {} if not hasattr(d, "labeled_tuples") else d.labeled_tuples
        d.labeled_cells = {} if not hasattr(d, "labeled_cells") else d.labeled_cells
        d.corrected_cells = {} if not hasattr(d, "corrected_cells") else d.corrected_cells
        return d

    def initialize_models(self, d):
        """
        This method initializes the error corrector models.
        """
        d.value_models = [{}, {}, {}, {}, {}]
        d.pdeps = {c: {cc: {} for cc in range(d.dataframe.shape[1])}
                   for c in range(d.dataframe.shape[1])}
        if os.path.exists(self.PRETRAINED_VALUE_BASED_MODELS_PATH):
            d.value_models = pickle.load(bz2.BZ2File(self.PRETRAINED_VALUE_BASED_MODELS_PATH, "rb"))
            if self.VERBOSE:
                print("The pretrained value-based models are loaded.")

        d.domain_models = {}
        d.lshs = {}
        for row in d.dataframe.itertuples():
            i, row = row[0], row[1:]
            # Das ist richtig cool: Jeder Wert des Tupels wird untersucht und
            # es wird ??berpr??ft, ob dieser Wert ein aus Error Detection bekannter
            # Fehler ist. Wenn dem so ist, wird der Wert durch das IGNORE_SIGN
            # ersetzt.
            vicinity_list = [cv if (i, cj) not in d.detected_cells else self.IGNORE_SIGN for cj, cv in enumerate(row)]
            for j, value in enumerate(row):
                # if rhs_value's position is not a known error
                if (i, j) not in d.detected_cells:
                    temp_vicinity_list = list(vicinity_list)
                    temp_vicinity_list[j] = self.IGNORE_SIGN
                    update_dictionary = {
                        "column": j,
                        "new_value": value,
                        "vicinity": temp_vicinity_list
                    }
                    self._domain_based_model_updater(d.domain_models, update_dictionary)
                    if "sim" in self.FEATURE_GENERATORS:
                        col_name = d.dataframe.columns[j]
                        if col_name not in d.lshs:
                            d.lshs[col_name] = MultiLSH()
                        d.lshs[col_name].add(value)

        # BEGIN Philipp's changes
        d.vicinity_models = {}
        for o in self.VICINITY_ORDERS:
            d.vicinity_models[o] = pdep.calculate_counts_dict(
                df=d.dataframe,
                detected_cells=d.detected_cells,
                order=o,
                ignore_sign=self.IGNORE_SIGN)
        d.imputer_models = {}

        self.df = pd.DataFrame(columns=d.dataframe.columns)
        for row in d.dataframe.itertuples():
            i, row = row[0], row[1:]
            vicinity_list = [cv if (i, cj) not in d.detected_cells else self.IGNORE_SIGN for cj, cv in enumerate(row)]
            self.df.loc[len(self.df)] = vicinity_list

        # d.newModel = Feature(self.df)

        if self.VERBOSE:
            print("The error corrector models are initialized.")

    def sample_tuple(self, d, random_seed):
        """
        This method samples a tuple.
        Philipp extended this with a random_seed to make runs reproducible.
        """
        rng = numpy.random.default_rng(seed=random_seed)
        remaining_column_erroneous_cells = {}
        remaining_column_erroneous_values = {}
        for j in d.column_errors:
            for cell in d.column_errors[j]:
                if cell not in d.corrected_cells:  # debug sampling?
                    self._to_model_adder(remaining_column_erroneous_cells, j, cell)
                    self._to_model_adder(remaining_column_erroneous_values, j, d.dataframe.iloc[cell])
        tuple_score = numpy.ones(d.dataframe.shape[0])
        tuple_score[list(d.labeled_tuples.keys())] = 0.0
        for j in remaining_column_erroneous_cells:
            for cell in remaining_column_erroneous_cells[j]:
                value = d.dataframe.iloc[cell]
                column_score = math.exp(len(remaining_column_erroneous_cells[j]) / len(d.column_errors[j]))
                cell_score = math.exp(
                    remaining_column_erroneous_values[j][value] / len(remaining_column_erroneous_cells[j]))
                tuple_score[cell[0]] *= column_score * cell_score
        # N??tzlich, um tuple-sampling zu debuggen: Zeigt die Tupel, aus denen
        # zuf??llig gew??hlt wird.
        # print(numpy.argwhere(tuple_score == numpy.amax(tuple_score)).flatten())
        d.sampled_tuple = rng.choice(numpy.argwhere(tuple_score == numpy.amax(tuple_score)).flatten())
        # @TODO was ist, wenn keine tupel mehr unkorrigiert sind?
        #    print("UMSCHREIBUNG")
        #    if len(d.old_scores) > 1:
        #        d.sampled_tuple = d.old_scores.pop()
        # else:
        #    if not hasattr(d, 'old_scores'):
        #        d.old_scores = []
        #    count = 0
        #    ind = numpy.argpartition(tuple_score, -4)[-4:]
        #    ind = ind[numpy.argsort(tuple_score[ind])]
        #    for v in ind:
        #        if tuple_score[v] > 1.0 and v not in d.old_scores:
        #            d.old_scores.append(v)
        #    if d.sampled_tuple in d.old_scores:
        #        d.old_scores.remove(d.sampled_tuple)
        # print(d.sampled_tuple)
        if self.VERBOSE:
            print("Tuple {} is sampled.".format(d.sampled_tuple))

    def label_with_ground_truth(self, d):
        """
        This method labels a tuple with ground truth.
        Takes the sampled row from d.sampled_tuple, iterates over each cell
        in that row taken from the clean data, and then adds {(row, col):
        [is_error, clean_value_from_clean_dataframe] to
        """
        d.labeled_tuples[d.sampled_tuple] = 1
        for col in range(d.dataframe.shape[1]):
            cell = (d.sampled_tuple, col)
            error_label = 0
            if d.dataframe.iloc[cell] != d.clean_dataframe.iloc[cell]:
                error_label = 1
                # print("Truths are: ", d.dataframe.iloc[cell], "->", d.clean_dataframe.iloc[cell])
            d.labeled_cells[cell] = [error_label, d.clean_dataframe.iloc[cell]]
        if self.VERBOSE:
            print("Tuple {} is labeled.".format(d.sampled_tuple))

    def update_models(self, d):
        """
        This method updates the error corrector models with a new labeled tuple.
        """
        cleaned_sampled_tuple = []
        for column in range(d.dataframe.shape[1]):
            clean_cell = d.labeled_cells[(d.sampled_tuple, column)][1]
            cleaned_sampled_tuple.append(clean_cell)
        # print("Cleaned", cleaned_sampled_tuple)
        for column in range(d.dataframe.shape[1]):
            cell = (d.sampled_tuple, column)
            update_dictionary = {
                "column": column,
                "old_value": d.dataframe.iloc[cell],
                "new_value": cleaned_sampled_tuple[column],
            }

            # if the value in that cell has been labelled an error
            if d.labeled_cells[cell][0] == 1:
                # update value and vicinity models.
                self._value_based_models_updater(d.value_models, update_dictionary)
                self._domain_based_model_updater(d.domain_models, update_dictionary)
                self._typ_based_models_updater(d.value_models, update_dictionary)
                update_dictionary["vicinity"] = [cv if column != cj else self.IGNORE_SIGN
                                                 for cj, cv in enumerate(cleaned_sampled_tuple)]

                # and the cell hasn't been detected as an error
                if cell not in d.detected_cells:
                    # add that cell to detected_cells and assign it IGNORE_SIGN
                    # --> das passiert, wenn die Error Detection nicht perfekt
                    # war, dass man einen Fehler labelt, der vorher noch nicht
                    # gelabelt war.
                    d.detected_cells[cell] = self.IGNORE_SIGN
                    self._to_model_adder(d.column_errors, cell[1], cell)

            else:
                update_dictionary["vicinity"] = [cv if column != cj and d.labeled_cells[(d.sampled_tuple, cj)][0] == 1
                                                 else self.IGNORE_SIGN for cj, cv in enumerate(cleaned_sampled_tuple)]

        # BEGIN Philipp's changes
        for o in self.VICINITY_ORDERS:
            pdep.update_counts_dict(d.dataframe,
                                    d.vicinity_models[o],
                                    o,
                                    cleaned_sampled_tuple)
        # END Philipp's changes

        if self.VERBOSE:
            print("The error corrector models are updated with new labeled tuple {}.".format(d.sampled_tuple))

    def _feature_generator_process(self, args):
        """
        This method generates cleaning suggestions for one error in one cell. The suggestion
        gets turned into features for the classifier in predict_corrections(). It gets called
        once for each each error cell.

        Depending on the value of `synchronous` in `generate_features()`, the method will
        be executed in parallel or not.
        """
        d, cell = args

        # vicinity ist die Zeile, column ist die Zeilennummer, old_value ist der Fehler
        error_dictionary = {"column": cell[1],
                            "old_value": d.dataframe.iloc[cell],
                            "vicinity": list(d.dataframe.iloc[cell[0], :]),
                            "row": cell[0]}
        naive_vicinity_corrections = []
        pdep_vicinity_corrections = []
        value_corrections = []
        domain_corrections = []
        imputer_corrections = []
        domain_new_corrections = []
        results_type_list = []

        # Begin Philipps Changes
        if "vicinity" in self.FEATURE_GENERATORS:
            if self.VICINITY_FEATURE_GENERATOR == 'naive':
                for o in self.VICINITY_ORDERS:
                    naive_corrections = pdep.vicinity_based_corrector_order_n(
                        counts_dict=d.vicinity_models[o],
                        ed=error_dictionary,
                        probability_threshold=self.MIN_CORRECTION_CANDIDATE_PROBABILITY)
                    naive_vicinity_corrections.append(naive_corrections)

            elif self.VICINITY_FEATURE_GENERATOR == 'pdep':
                for o in self.VICINITY_ORDERS:
                    pdep_corrections = pdep.pdep_vicinity_based_corrector(
                        inverse_sorted_gpdeps=d.inv_vicinity_gpdeps[o],
                        counts_dict=d.vicinity_models[o],
                        ed=error_dictionary,
                        score_threshold=self.GPDEP_CORRECTION_SCORE_THRESHOLD,
                        n_best_pdeps=self.N_BEST_PDEPS)
                    pdep_vicinity_corrections.append(pdep_corrections)
            else:
                raise ValueError(f'Unknown VICINITY_FEATURE_GENERATOR '
                                 f'{self.VICINITY_FEATURE_GENERATOR}')

        col_name = d.dataframe.columns[error_dictionary["column"]]

        if "value" in self.FEATURE_GENERATORS:
            value_corrections, results_type_list = self._value_based_corrector(d.value_models, error_dictionary)
        # if "type" in self.FEATURE_GENERATORS:
        if "domain" in self.FEATURE_GENERATORS:
            domain_corrections = self._domain_based_corrector(d.domain_models, error_dictionary)
        if "sim" in self.FEATURE_GENERATORS:
            if col_name in d.lshs:
                # domain_new_corrections = self._domain_based_corrector_sim(error_dictionary["old_value"], self.df[col_name].to_list())
                domain_new_corrections = self._domain_based_corrector_sim_2(error_dictionary["old_value"],
                                                                            d.lshs[col_name])
                # domain_new_corrections = self._domain_based_corrector_sim_3(error_dictionary["old_value"], self.df[col_name].to_list())
        if "imputer" in self.FEATURE_GENERATORS:
            imputer_corrections = self._imputer_based_corrector(d.imputer_models, error_dictionary)

        # print("Suggestions for", error_dictionary["old_value"], ":", "V", value_corrections, "D", domain_corrections, "P", pdep_vicinity_corrections, "N",naive_vicinity_corrections)
        # col_name = d.dataframe.columns[error_dictionary["column"]]
        # full_row = error_dictionary["vicinity"]
        # print("Suggestions for", error_dictionary["old_value"], d.newModel.getCorrectionFor(col_name, d.dataframe.columns.tolist(), full_row), pdep_vicinity_corrections)
        # if not results_type_list or not results_type_list[0]:
        models_corrections = value_corrections + domain_corrections \
                             + [corrections for order in naive_vicinity_corrections for corrections in order] \
                             + [corrections for order in pdep_vicinity_corrections for corrections in order] \
                             + imputer_corrections
        if "sim" in self.FEATURE_GENERATORS:
            models_corrections = models_corrections + domain_new_corrections
        if "type" in self.FEATURE_GENERATORS:
            models_corrections = models_corrections + results_type_list
        # else:
        #    models_corrections = results_type_list + value_corrections + domain_corrections \
        #        + [corrections for order in naive_vicinity_corrections for corrections in order] \
        #        + [corrections for order in pdep_vicinity_corrections for corrections in order] \
        ##        + imputer_corrections
        # End Philipps Changes
        # self.feature.verify(error_dictionary["old_value"], col_name, error_dictionary["row"], models_corrections)
        corrections_features = {}
        for mi, model in enumerate(models_corrections):
            for correction in model:
                if correction not in corrections_features:
                    corrections_features[correction] = numpy.zeros(len(models_corrections))

                if "rule" in self.FEATURE_GENERATORS:
                    if (error_dictionary["old_value"], correction) not in self.already_verified:
                        bool_verify = self.feature.verify_value(col_name, error_dictionary["row"],
                                                                error_dictionary["old_value"],
                                                                correction)
                        self.already_verified[(error_dictionary["old_value"], correction)] = bool_verify
                    if not self.already_verified[(error_dictionary["old_value"], correction)]:
                        # print(correction, "should not be considered for", error_dictionary["old_value"], model[correction])
                        corrections_features[correction][mi] = model[correction]/2
                    else:
                        corrections_features[correction][mi] = model[correction]

                else:
                    # if model[correction] > 0.9:
                    #    corrections_features[correction][mi] = 1.0
                    # else:
                    corrections_features[correction][mi] = model[correction]
        # print(corrections_features)
        return corrections_features

    def generate_features(self, d, synchronous=False):
        """
        This method generates a feature vector for each pair of a data error
        and a potential correction.
        Philipp added a `synchronous` parameter to make debugging easier.
        """

        d.create_repaired_dataset(d.corrected_cells)

        # Calculate gpdeps and append them to d
        if self.VICINITY_FEATURE_GENERATOR == 'pdep':
            d.inv_vicinity_gpdeps = {}
            for o in self.VICINITY_ORDERS:
                vicinity_gpdeps = pdep.calc_all_gpdeps(d.vicinity_models[o],
                                                       d.repaired_dataframe)
                d.inv_vicinity_gpdeps[o] = pdep.invert_and_sort_gpdeps(vicinity_gpdeps)

        # train imputer model for each column.
        if 'imputer' in self.FEATURE_GENERATORS:
            df_clean_subset = imputer.get_clean_table(d.dataframe, d.detected_cells)
            for i_col, col in enumerate(df_clean_subset.columns):
                imp = imputer.train_cleaning_model(df_clean_subset,
                                                   d.name,
                                                   label=i_col,
                                                   time_limit=45,
                                                   use_cache=self.IMPUTER_CACHE_MODEL)
                if imp is not None:
                    d.imputer_models[i_col] = imp.predict_proba(d.dataframe)
                else:
                    d.imputer_models[i_col] = None

        d.pair_features = {}
        pairs_counter = 0
        process_args_list = [[d, cell] for cell in d.detected_cells]
        if not synchronous:
            pool = multiprocessing.Pool()
            feature_generation_results = pool.map(self._feature_generator_process, process_args_list)
            pool.close()
        else:
            feature_generation_results = []
            for args_list in process_args_list:
                result = self._feature_generator_process(args_list)
                feature_generation_results.append(result)

        for ci, corrections_features in enumerate(feature_generation_results):
            cell = process_args_list[ci][1]
            d.pair_features[cell] = {}
            for correction in corrections_features:
                d.pair_features[cell][correction] = corrections_features[correction]
                pairs_counter += 1

        if self.VERBOSE:
            print("{} pairs of (a data error, a potential correction) are featurized.".format(pairs_counter))

    def predict_corrections(self, d, random_seed=None):
        """
        This method predicts

        Philipp added support for a random_seed to all classifiers. When the random_seed is set,
        we measure a steep decline in cleaning performance. So not a recommended feature.
        """
        for j in d.column_errors:
            x_train = []
            y_train = []
            x_test = []
            test_cell_correction_list = []
            for _, cell in enumerate(d.column_errors[j]):
                if cell in d.pair_features:
                    for correction in d.pair_features[cell]:
                        if cell in d.labeled_cells and d.labeled_cells[cell][0] == 1:
                            x_train.append(d.pair_features[cell][correction])
                            y_train.append(int(correction == d.labeled_cells[cell][1]))
                            d.corrected_cells[cell] = d.labeled_cells[cell][1]
                        else:
                            x_test.append(d.pair_features[cell][correction])
                            test_cell_correction_list.append([cell, correction])
            if self.CLASSIFICATION_MODEL == "ABC":
                classification_model = sklearn.ensemble.AdaBoostClassifier(n_estimators=100, random_state=random_seed)
            if self.CLASSIFICATION_MODEL == "DTC":
                classification_model = sklearn.tree.DecisionTreeClassifier(criterion="gini", random_state=random_seed)
            if self.CLASSIFICATION_MODEL == "GBC":
                classification_model = sklearn.ensemble.GradientBoostingClassifier(n_estimators=100)
            if self.CLASSIFICATION_MODEL == "GNB":
                classification_model = sklearn.naive_bayes.GaussianNB()
            if self.CLASSIFICATION_MODEL == "KNC":
                classification_model = sklearn.neighbors.KNeighborsClassifier(n_neighbors=1)
            if self.CLASSIFICATION_MODEL == "SGDC":
                classification_model = sklearn.linear_model.SGDClassifier(loss="hinge", penalty="l2")
            if self.CLASSIFICATION_MODEL == "SVC":
                classification_model = sklearn.svm.SVC(kernel="sigmoid")

            if len(x_train) > 0 and len(x_test) > 0:
                if sum(y_train) == 0:
                    predicted_labels = numpy.zeros(len(x_test))
                elif sum(y_train) == len(y_train):
                    predicted_labels = numpy.ones(len(x_test))
                else:
                    classification_model.fit(x_train, y_train)
                    predicted_labels = classification_model.predict(x_test)

                for index, predicted_label in enumerate(predicted_labels):
                    cell, predicted_correction = test_cell_correction_list[index]
                    if predicted_label:
                        d.corrected_cells[cell] = predicted_correction
            elif len(d.labeled_tuples) == 0:  # no training data because no user labels
                for cell in d.pair_features:
                    correction_dict = d.pair_features[cell]
                    if len(correction_dict) > 0:
                        max_proba_feature = \
                        sorted([v for v in correction_dict.items()], key=lambda x: sum(x[1]), reverse=True)[0]
                        d.corrected_cells[cell] = max_proba_feature[0]

        # print(d.corrected_cells)
        # print(d.detected_cells)
        # if self.VERBOSE:
        print("{:.0f}% ({} / {}) of data errors are corrected.".format(
            100 * len(d.corrected_cells) / len(d.detected_cells),
            len(d.corrected_cells), len(d.detected_cells)))

    def store_results(self, d):
        """
        This method stores the results.
        """
        ec_folder_path = os.path.join(d.results_folder, "error-correction")
        if not os.path.exists(ec_folder_path):
            os.mkdir(ec_folder_path)
        pickle.dump(d, open(os.path.join(ec_folder_path, "correction.dataset"), "wb"))
        if self.VERBOSE:
            print("The results are stored in {}.".format(os.path.join(ec_folder_path, "correction.dataset")))

    def run(self, d, random_seed):
        """
        This method runs Baran on an input dataset to correct data errors.
        """
        if self.VERBOSE:
            print("------------------------------------------------------------------------\n"
                  "---------------------Initialize the Dataset Object----------------------\n"
                  "------------------------------------------------------------------------")
        d = self.initialize_dataset(d)
        if self.VERBOSE:
            print("------------------------------------------------------------------------\n"
                  "--------------------Initialize Error Corrector Models-------------------\n"
                  "------------------------------------------------------------------------")
        self.initialize_models(d)
        if self.VERBOSE:
            print("------------------------------------------------------------------------\n"
                  "--------------Iterative Tuple Sampling, Labeling, and Learning----------\n"
                  "------------------------------------------------------------------------")
        pre = []
        recall = []
        f1 = []

        df_clean_subset = imputer.get_clean_table(d.dataframe, d.detected_cells)
        if "norm" in self.FEATURE_GENERATORS or "sim" in self.FEATURE_GENERATORS or "rule" in self.FEATURE_GENERATORS:
            self.feature = Feature(df_clean_subset, d.dataframe, self.FEATURE_GENERATORS)
        while len(d.labeled_tuples) < self.LABELING_BUDGET:
            self.sample_tuple(d, random_seed=random_seed)
            if d.has_ground_truth:
                self.label_with_ground_truth(d)
            else:
                ValueError('go label tuples manually in a jupyter notebook.')
            self.update_models(d)
            self.generate_features(d, synchronous=True)
            self.predict_corrections(d, random_seed=random_seed)
            p, r, f = data.get_data_cleaning_evaluation(d.corrected_cells, d.dataframe)[-3:]
            # print("Corrected: ", list(d.corrected_cells.items())[:4])
            print(
                "Baran's performance on {}:\nPrecision = {:.2f}\nRecall = {:.2f}\nF1 = {:.2f}".format(d.name, p, r, f))
            pre.append(round(p, 2))
            recall.append(round(r, 2))
            f1.append(round(f, 2))

        print(d.get_errors())
        print(len(d.get_errors()))
        print("Recall_1 = ", recall)
        print("Precision_1 = ", pre)
        print("F1_1 = ", f1)
        return d.corrected_cells


########################################


########################################
if __name__ == "__main__":
    dataset_name = "hospital"
    dataset_dictionary = {
        "name": dataset_name,
        "path": os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir, "datasets", dataset_name, "dirty.csv")),
        "clean_path": os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir, "datasets", dataset_name, "clean.csv"))
    }
    data = raha.dataset.Dataset(dataset_dictionary)
    data.detected_cells = dict(data.get_actual_errors_dictionary())
    app = Correction()
    app.LABELING_BUDGET = 20

    app.VICINITY_ORDERS = [1]
    app.VICINITY_FEATURE_GENERATOR = "pdep"
    app.N_BEST_PDEPS = 5
    app.GPDEP_CORRECTION_SCORE_THRESHOLD = 0.00
    app.SAVE_RESULTS = False
    # "sim" "type" "rule"
    app.FEATURE_GENERATORS = ['value', 'domain', 'vicinity', 'rule', 'type', 'sim']
    app.IMPUTER_CACHE_MODEL = True
    app.VERBOSE = False
    seed = None

    start = time.time()
    correction_dictionary = app.run(data, seed)
    end = time.time()
    print(end - start, "sec")
    # p, r, f = data.get_data_cleaning_evaluation(correction_dictionary)[-3:]
    # print("Baran's performance on {}:\nPrecision = {:.2f}\nRecall = {:.2f}\nF1 = {:.2f}".format(data.name, p, r, f))

    # --------------------
    # app.extract_revisions(wikipedia_dumps_folder="../wikipedia-data")
    # app.pretrain_value_based_models(revision_data_folder="../wikipedia-data/revision-data")
########################################
