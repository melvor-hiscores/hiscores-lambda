from getranks import lambda_handler

import json

lambda_name = 'getranks'

def main():
    with open('test_data/{}.json'.format(lambda_name), 'r') as json_file:
        lambda_input_json = json.load(json_file)

        print('Response: ' + str(lambda_handler(lambda_input_json, None)))

main()
