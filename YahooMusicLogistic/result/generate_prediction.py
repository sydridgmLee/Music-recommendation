prediction_file = "prediction.csv"
probility_file = "probility1.csv"

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
    with open(probility_file) as probility_data:
        prob_lines = read_lines(probility_data, 6)
        while prob_lines:
            probs = []
            for line in prob_lines:
                prob = line.strip("\n")
                probs.append(prob)

            sorted_list = sorted(probs)
            # print(sorted_list)
            i = 0
            predictions = {}
            for item in sorted_list:
                if i < 3:
                    predictions[item] = 0
                else:
                    predictions[item] = 1
                i += 1

            for prob in probs:
                prediction_data.write(str(predictions[prob]) + "\n")

            prob_lines = read_lines(probility_data, 6)
