def omit_header(row):
    return not row.startswith('jugador_id')

def parse_jugador_func(line):
    cols = line.split(',')
    return (int(cols[0]), cols[1], cols[2], int(cols[3]), int(cols[4]), int(cols[5]), cols[6], int(cols[7]))

def filter_mayor_edad(line):
    return int(line.split(',')[3]) > 30

def filter_defensa(line):
    return line.split(',')[6].strip() == 'Defensa'

def a_mayusculas_func(line):
    cols = line.split(',')
    cols[1] = cols[1].upper()
    cols[2] = cols[2].upper()
    return ','.join(cols)
