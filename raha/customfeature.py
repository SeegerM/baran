import itertools
import re
from collections import defaultdict


class Feature:
    """
    The main class.
    """

    def __init__(self, d, old_frame, features):
        # depMap = self.computeFD(d.clean_dataframe)
        self.verbose = False
        self.error_lvl = 0.00
        self.improvements = 0
        self.nullValues = ["", "N/A"]
        self.enums = self.compute_enum(d)
        self.uccMap = self.compute_ucc(d)
        self.depMap = self.compute_fd(d)
        if "norm" in features:
            self.normalize(self.depMap, d)
        self.d = old_frame
        self.newFrame = d
        #print(self.depMap)
        self.typeMap = self.compute_ccc(d)
        self.typeMap["ounces"] = "float"


    def getCorrectionFor(self, column_name, columns, row):
        corrections = []
        for i in self.depMap:
            if column_name in i[1]:
                map = self.depMap[i]
                if row[columns.index(i[0])] in map:
                    value = map[row[columns.index(i[0])]]
                    corrections.append(value)
        return corrections

    def compute_enum(self, d):
        col_map = {}
        allowed_false = len(d) * 0.2
        for col in d:
            enum_col_map = {}
            for i, row_value in d[col].iteritems():
                if len(enum_col_map) > allowed_false:
                    break
                if row_value not in enum_col_map:
                    enum_col_map[row_value] = 0
                enum_col_map[row_value] = enum_col_map[row_value]+1
            if len(enum_col_map) < len(d) * 0.2:
                if self.verbose:
                    print("Enum found: ", col)
                col_map[col] = enum_col_map.keys()
        return col_map

    def compute_ucc(self, d):
        ucc_col_map = {}
        allowed_false = len(d) * self.error_lvl
        current_false = 0
        for col in d:
            if col in self.enums:
                ucc_col_map[col] = False
                continue
            s = set()
            for i, row_value in d[col].iteritems():
                if row_value in s:
                    current_false = current_false + 1
                    if current_false > allowed_false:
                        ucc_col_map[col] = False
                        break
                s.add(row_value)
            if col not in ucc_col_map:
                ucc_col_map[col] = True
                if self.verbose:
                    print(col, "is a UCC")
        return ucc_col_map

    def compute_ccc(self, d):
        typ_col_map = {}
        for col in d:
            typ_map = {"str": 0, "int": 0, "float": 0}
            for i, row_value in d[col].iteritems():
                if re.match(r'^-?\d+(?:\.\d+)$', row_value) is not None:
                    typ_map["float"] = typ_map["float"] + 1
                elif row_value.isdigit():
                    typ_map["int"] = typ_map["int"] + 1
                else:
                    typ_map["str"] = typ_map["str"] + 1
            most_used_typ = "str"
            if typ_map["str"] < typ_map["float"] and typ_map["int"] < typ_map["float"]:
                most_used_typ = "float"
            elif typ_map["str"] < typ_map["int"] and typ_map["float"] < typ_map["int"]:
                most_used_typ = "int"
            if self.verbose:
                print(col, " is ", most_used_typ)
            typ_col_map[col] = most_used_typ
        return typ_col_map

    ignore_sign = "<<<IGNORE_THIS_VALUE>>>"

    def compute_fd(self, d):
        depMap = defaultdict(list)
        depList = {}
        for leftCol in d.columns:
            if self.uccMap[leftCol]:
                continue
            for rightCol in d.columns:
                if leftCol == rightCol:
                    continue
                dep_bool, map = self.check(d, leftCol, rightCol)

                if dep_bool:
                    if not leftCol in depMap:
                        depMap[leftCol] = list()
                    depMap[leftCol].append(rightCol)
                    depList[(leftCol, rightCol)] = map
                    if self.verbose:
                        print("Functional Dependency: ", leftCol, "->", rightCol)
        return depList

    def check(self, d, leftCol, rightCol):
        map = {}
        allowed_false = len(d) * self.error_lvl
        current_false = 0
        for a, b in zip(d[leftCol], d[rightCol]):
            if a in self.nullValues or b in self.nullValues or a == self.ignore_sign or b == self.ignore_sign:
                continue
            if a in map and map[a] != b:
                if current_false > allowed_false:
                    return False, {}
                else:
                    current_false = current_false + 1
                    map[a] = b
            else:
                map[a] = b
        return True, map

    def checkForImprovement(self, clean_dataframe, dirty, actual, expected, cell):
        col_name = clean_dataframe.columns[cell[1]]
        old_value = dirty.iloc[cell[0]][col_name]
        for left_col in clean_dataframe.columns:
            if (left_col, col_name) in self.depMap:
                row = clean_dataframe.iloc[cell[0]].T
                left_key = clean_dataframe.iloc[cell[0]][left_col]
                mydict = self.depMap[(left_col, col_name)]
                corrected_value = mydict[left_key]
                if old_value != corrected_value:
                    if corrected_value == expected:
                        if self.verbose:
                            print("FD improvement: ", corrected_value, " Old Correction: ", actual, " Actual: ", expected, " Wrong Value: ", old_value)
                        self.improvements = self.improvements + 1
                        return True
        return self.cccImprove(col_name, old_value, expected, actual) or self.enumImprovement(col_name, old_value, expected, actual)

    def enumImprovement(self, col_name, old_value, expected, actual):
        if col_name not in self.enums:
            return False
        for x in old_value.split():
            if x in self.enums[col_name]:
                if x == expected:
                    if self.verbose:
                        print("Enum improvement: ", x, " Old Correction: ", actual, " Actual: ", expected,
                              " Wrong Value: ", old_value)
                    self.improvements = self.improvements + 1
                    return True
        return False

    def cccImprove(self, col_name, old_value, expected, actual):
        typ = self.typeMap[col_name]
        corrected_value = ""
        if typ == "str":
            corrected_value = re.sub('\d', '', old_value)
        elif typ == "int":
            corrected_value = re.sub('\D', '', old_value).replace(" ","")
        elif typ == "float":
            corrected_value = re.sub('[^0-9.]', '', old_value).replace(" ","")
        if expected == corrected_value:
            if self.verbose:
                print("CCC improvement: ", corrected_value, " Old Correction: ", actual, " Actual: ", expected, " Wrong Value: ", old_value)
            self.improvements = self.improvements+1
            return True
        return False

    def printImrpovement(self, prf):
        if self.verbose:
            print('\x1b[6;30;42m' + "Feature found better value: ", self.improvements, " prf: ", prf, '\x1b[0m')
        #else:
        #    print("Feature: Better Values = {}, Precision = {:.2f}, Recall = {:.2f}, F1 = {:.2f}".format(self.improvements, prf[0], prf[1], prf[2]))
    #    row = self.clean_dataframe.iloc[[cell[0]]].T
    #    print("Correction: ", correction_dictionary[cell], " Actual: ", actual_errors[cell])
    #    print("Error in col: " + self.clean_dataframe.columns[cell[1]])
    #    print("Row: ", row)
    #    print("----------------------")

    def verify(self, old_value, col_name, row, corrections):
        #print("Old_Value:", old_value, "in", col_name)
        for x in corrections:
            if len(x) == 0:
                continue
            for value in x:
                self.verify_value(col_name, row, old_value, value)

    def verify_value(self, col_name, row, old_v, new_v, skip_fd = True):
        #if col_name in self.uccMap and self.uccMap[col_name]:
        #    return new_v not in self.newFrame[col_name]
        #if col_name in self.enums and new_v not in self.enums[col_name]:
        #    return False
        #if skip_fd:
        #    return True
        for col_pair in self.depMap:
            first = col_pair[0]
            sec = col_pair[1]
            if sec == col_name:
                #print(row, first)
                left_side_value = self.d.iloc[row][first]
                if left_side_value not in self.depMap[col_pair]:
                    continue
                right_side_value = self.depMap[col_pair][left_side_value]
                if old_v != right_side_value and right_side_value != new_v:
                    if self.verbose:
                        print(new_v, "can not be a valid value for", old_v, left_side_value, right_side_value)
                    return False
        return True

    def normalize(self, depMap, dataframe):
        already_checked = []
        norm_map = {}
        for col_pair in depMap:
            first = col_pair[0]
            sec = col_pair[1]
            new_col_pair = (sec, first)
            if new_col_pair in depMap and new_col_pair not in already_checked:
                #print(sec, "and", first, "can be normailzed!")
                already_checked.append(col_pair)
                if first not in norm_map:
                    norm_map[first] = list()
                norm_map[first].append(sec)
                if sec not in norm_map:
                    norm_map[sec] = list()
                norm_map[sec].append(first)

        sorted_norm_map = dict(sorted(norm_map.items(), key=lambda item: len(item[1])))
        list_of_dicts = []
        rest_cols = list(dataframe.columns)
        while sorted_norm_map:
            first_col = next(iter(sorted_norm_map))
            list_of_cols = sorted_norm_map[first_col]
            list_of_cols.append(first_col)
            df_copy = dataframe[list_of_cols].copy()
            df_copy.drop_duplicates(keep='first', inplace=True)
            list_of_dicts.append(df_copy)
            for v in list_of_cols:
                del sorted_norm_map[v]
                rest_cols.remove(v)

        df_copy = dataframe[rest_cols].copy()
        df_copy.drop_duplicates(keep='first', inplace=True)
        list_of_dicts.append(df_copy)

        #x = self.create_sub_table(['City', 'CountyName'], dataframe)
       # x1 = self.create_sub_table(['State', 'CountyName', 'StateAverage'], dataframe)
        #x2 = self.create_sub_table(['EmergencyService', 'CountyName'], dataframe)
        #x3 = self.create_sub_table(['StateAverage', 'Condition'], dataframe)
        #x4 = self.create_sub_table(['City', 'HospitalOwner', 'Score', 'Sample', 'StateAverage'], dataframe)
        #for col_pair in depMap:
        #    first = col_pair[0]
        #    sec = col_pair[1]
        #    if first in rest_cols and sec in rest_cols:
        #        print(first, sec)

        for norm_dict in list_of_dicts:
            print("Normalized: ", list(norm_dict.columns))
        print("Done")

    def create_sub_table(self, list_cols, dataframe):
        df_copy = dataframe[list_cols].copy()
        df_copy.drop_duplicates(keep='first', inplace=True)
        return df_copy
