import nltk


class LSH:

    def __init__(self, fkt=None):
        if fkt is None:
            fkt = ["first", "len", "last"]
        self.dict = {}
        self.fkt = fkt

    def add(self, value):
        hash_value = self.hash(value)
        if hash_value not in self.dict:
            self.dict[hash_value] = set()
        self.dict[hash_value].add(value)

    def get(self, value):
        hash_value = self.hash(value)
        if hash_value not in self.dict:
            return set()
        return self.dict[hash_value]

    def buckets(self):
        return len(self.dict)

    def hash(self, value):
        if not value or len(value) == 0:
            return -1
        if "first" in self.fkt:
            return value[0]
        elif "last" in self.fkt:
            return value[-1]
        elif "len" in self.fkt:
            return len(value)
        else:
            return -1

    def sim(self, value):
        return get_min_sim(self.get(value), value)


class MultiLSH:

    def __init__(self, tokenize=False, fkt=None):
        self.tokenize = tokenize
        if fkt is None:
            fkt = ["first", "len", "last"]
        self.lhss = []
        for x in fkt:
            self.lhss.append(LSH([x]))

    def add(self, value):
        if self.tokenize:
            for split in value.split(" "):
                for lhs in self.lhss:
                    lhs.add(split)
        for lhs in self.lhss:
            lhs.add(value)

    def get(self, value):
        result = set()
        backup = set()
        for lhs in self.lhss:
            if lhs.buckets() <= 1:
                backup = lhs.get(value)
                continue
            result = result.union(lhs.get(value))
        if len(result) > 0:
            return result
        return backup

    number_of_compares = 0
    number = 0
    already_computed_dict = {}

    def avg_comp(self):
        if self.number == 0 or self.number_of_compares == 0:
            return 0
        return self.number_of_compares / self.number

    def sim(self, value):
        if self.tokenize:
            return self.sim_token(value)
        return self.sim_normal(value)

    def sim_normal(self, value):
        self.number = self.number + 1
        if value in self.already_computed_dict:
            return self.already_computed_dict[value]
        sim_values = self.get(value)
        self.number_of_compares = self.number_of_compares + len(sim_values)
        min_value, sim_score = get_min_sim(sim_values, value)
        self.already_computed_dict[value] = (min_value, sim_score)
        return min_value, sim_score

    def sim_token(self, value):
        self.number = self.number + 1
        result = []
        splits = value.split(" ")
        if len(splits) == 1:
            return self.sim_normal(value)
        for split in splits:
            if split in self.already_computed_dict:
                result.append(self.already_computed_dict[split])
            values = self.get(split)
            self.number_of_compares = self.number_of_compares + len(values)
            min_value, sim_score = get_min_sim(split, value)
            self.already_computed_dict[split] = (min_value, sim_score)
            result.append((min_value, sim_score))

        min_value, sim_score = self.sim_normal(value)
        avg = 0
        result_string = ""
        for x in result:
            avg += x[1]
            result_string += x[0]
        if avg/len(result) > sim_score:
            return result_string, avg/len(result)
        return min_value, sim_score


def get_min_sim(list_of_values, value):
    min_value = ""
    min_dis = 10000000000.0
    for l_value in list_of_values:
        dist = nltk.edit_distance(value, l_value)
        if dist < min_dis:
            min_value = l_value
            min_dis = dist
    if min_value == "":
        return None, 0
    return min_value, 1 - min_dis / len(min_value)


if __name__ == "__main__":
    lsh = MultiLSH()
    values = ["Max Müller", "Meier  Müller", "Test  Müller", "Müller x", "12345 Meier"]
    for v in values:
        lsh.add(v)

    print(lsh.sim("Meier"))
