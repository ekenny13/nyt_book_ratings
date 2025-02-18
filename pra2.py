def split_and_join(line):
    # write your code here
    split_line = line.split(" ")
    new_line = []
    end = len(split_line)
    for spl in split_line:
        new_line.append(spl)
        new_line.append("-")

    return ''.join(str(n) for n in new_line[:-1])

def print_full_name(first, last):
    # Write your code here
    print('Hello ' + first + ' ' + last + '! You just delved into python.')

def count_substring(string, sub_string):
    count_ofsub = 0
    len_substring = len(sub_string)
    for x in range(len(string)):
        if string[x:x+len_substring] == sub_string:
            count_ofsub = count_ofsub + 1
    return count_ofsub

def swap_case(s):
    new_string = ''
    for x in s:
        if x.isupper() is True:
            new_string = new_string + x.lower()
        else:
            new_string = new_string + x.upper()
    return new_string

if __name__ == '__main__':
   # print(split_and_join("this is a string"))

    #first_name = input()
   # last_name = input()
   # print_full_name(first_name, last_name)

    print(swap_case('heLLo'))

thickness = int(input())  # This must be an odd number
c = 'H'

# Top Cone
for i in range(thickness):
    print((c * i).rjust(thickness - 1) + c + (c * i).ljust(thickness - 1))
#Top Pillars
for i in range(thickness+1):
    print((c*thickness).center(thickness*2)+(c*thickness).center(thickness*6))

#Middle Belt
for i in range((thickness+1)//2):
    print((c*thickness*5).center(thickness*6))
#Bottom Pillars
for i in range(thickness+1):
    print((c*thickness).center(thickness*2)+(c*thickness).center(thickness*6))
#Bottom Cone
for i in range(thickness):
    print(((c*(thickness-i-1)).rjust(thickness)+c+(c*(thickness-i-1)).ljust(thickness)).rjust(thickness*6))