# Project Name: An Elastic Real-Time Stream Processing System Based on K8s
#
# Team Member: Zhuangwei Kang, Manyao Peng, Yingqi, Li, Minhui, Zhou
#
# File Name: DataGenerator.py
# NOTE: This file is used to generate input data. Generated data are stored under DataSource
# directory, each file contains 1000000 random numbers. And there are 50 data source files.
#
import random


class DataGenerator:
    def __init__(self):
        states = 'Alabama,Alaska,Arizona,Arkansas,California,Colorado,Connecticut,Delaware,Florida,Georgia,Hawaii,Idaho,Illinois Indiana,Iowa,Kansas,Kentucky,Louisiana,Maine,Maryland,Massachusetts,Michigan,Minnesota,Mississippi,Missouri,Montana Nebraska,Nevada,New Hampshire,New Jersey,New Mexico,New York,North Carolina,North Dakota,Ohio,Oklahoma,Oregon,Pennsylvania Rhode Island,South Carolina,South Dakota,Tennessee,Texas,Utah,Vermont,Virginia,Washington,West Virginia,Wisconsin,Wyoming'
        self.states = states.split(',')

    def generator(self):
        for state in self.states:
            path = './DataSource/' + state + '.txt'
            with open(path, 'w') as file:
                for i in range(1000000):
                    file.write(str(random.randint(1, 10000)) + '\n')


if __name__ == '__main__':
    s = DataGenerator()
    s.generator()