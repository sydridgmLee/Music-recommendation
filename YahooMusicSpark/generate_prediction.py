prediction_file = "result/prediction.csv"
rate_file = "result/final_threshold_result.txt"

def read_lines(file, num):
    lines = []
    line = file.readline()
    lines.append(line)
    if line:
        for i in range(1,num):
            lines.append(file.readline())
        return lines
    else:
        return line

with open(prediction_file, 'w') as prediction_data:
    with open(rate_file) as rate_data:
        rate_lines = read_lines(rate_data, 6)
        while rate_lines:
            rates = []
            for line in rate_lines:
                rate = line.strip("\n")
                rates.append(rate)

            sorted_list = sorted(rates)
            # print(sorted_list)
            i = 0
            predictions = {}
            for item in sorted_list:
                if i < 3:
                    predictions[item] = 0
                else:
                    predictions[item] = 1
                i += 1

            for rate in rates:
                prediction_data.write(str(predictions[rate]) + "\n")

            rate_lines = read_lines(rate_data, 6)
