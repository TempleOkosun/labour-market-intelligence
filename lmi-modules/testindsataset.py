# Default display code
class Dataset:
    def __init__(self, data):
        self.header = data[0]
        self.data = data[1:]

    # Add the special method here

    def column(self, label):
        if label not in self.header:
            return None

        index = 0
        for idx, element in enumerate(self.header):
            if label == element:
                index = idx

        column = []
        for row in self.data:
            column.append(row[index])
        return column

    def count_unique(self, label):
        unique_results = set(self.column(label))
        count = len(unique_results)
        return count


nfl_dataset = Dataset(nfl_data)