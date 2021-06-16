if __name__ == '__main__':

    file_name = "lj_target.csv"
    with open(file_name, 'w') as file:
        file.write("id,attr\n")
        for i in range(3997962):
            file.write(str(i)+",0\n")