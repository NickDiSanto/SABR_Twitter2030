import time

time.sleep(120)

print('finished sleeping, printing output')

with open('TEST_NOHUP.txt', 'w') as new_file:
    new_file.write('testing to see if nohup works')