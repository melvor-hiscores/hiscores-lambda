import base64
import boto3
import math
import json
import zlib

client = boto3.client('dynamodb')

skill_index_dict = {
    "total": 0,
    "attack": 1,
    "strength": 2,
    "defence": 3,
    "hitpoints": 4,
    "ranged": 5,
    "magic": 6,
    "prayer": 7,
    "slayer": 8,
    "woodcutting": 9,
    "fishing": 10,
    "firemaking": 11,
    "cooking": 12,
    "mining": 13,
    "smithing": 14,
    "thieving": 15,
    "farming": 16,
    "fletching": 17,
    "crafting": 18,
    "runecrafting": 19,
    "herblore": 20
}

def extract_skill(event):

    # for parsing skill
    QUERY_PARAMETERS_KEY = 'queryStringParameters'
    SKILL_KEY = 'skill'
    FMT_SKILL = '{ \"' + SKILL_KEY + '\": \"<skill>\" }'
    if (QUERY_PARAMETERS_KEY not in event.keys()):
        raise Exception('500 Internal Server Error | APIGateway must have \'' + QUERY_PARAMETERS_KEY + '\' as a key')

    try:
        return event[QUERY_PARAMETERS_KEY][SKILL_KEY]
    except Exception:
        raise Exception('400 Bad Request | APIGateway skill must be in the format : ' + FMT_SKILL)

def convert_data(data):
    decoded = base64.b64decode(data)
    unzipped = zlib.decompress(decoded, 15+32) # 15+32 is magic, somehow detects if zlib or gzip format
    return unzipped.decode("utf-8")

def get_user_json_from_scan_for_index(scan_result, index):
    return scan_result['Items'][index]

def get_data_json_from_user_json(user_json):
    return json.loads(convert_data(user_json['data']['S']))

def equate(xp):
    return math.floor(xp + 300 * math.pow(2, xp / 7))

def level_to_xp(level):
    xp = 0
    for i in range(1, int(level)+1):
        xp += equate(i)
    return math.floor(xp / 4)

def xp_to_virtual_level(xp):
    level = 1
    while level_to_xp(level) < int(xp):
        level += 1
    return level

def lambda_handler(event, context):

    try:

        queryParam = extract_skill(event).lower()
        print('queryParam: ' + queryParam)
        skill_index = skill_index_dict[queryParam]

        scan_result = client.scan(
            TableName='MelvorHiscores'
        )

        users_tuple_list = []

        # for each user, put their user index in the bucket for their xp in the query param skill
        for each_user_index in range(0, len(scan_result['Items'])):
            each_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
            #print('each_user: ' + str(each_user_json))
            skill_xp = get_data_json_from_user_json(each_user_json)['skillXP'][skill_index]
            skill_level = get_data_json_from_user_json(each_user_json)['skillLevel'][skill_index]
            if queryParam != 'total' and int(skill_level) >= 99:
                skill_level = xp_to_virtual_level(skill_xp)
                print('virtual level: ' + str(skill_level))
            name = get_data_json_from_user_json(each_user_json)['username']

            each_user_tuple = (skill_level, skill_xp, name, each_user_index)
            users_tuple_list.append(each_user_tuple)

        print(str(users_tuple_list))

        s = sorted(users_tuple_list, key = lambda x: (x[0], x[1], x[2]), reverse=True)

        sorted_results = []

        for i, each_user_tuple in enumerate(s):
            each_user_index = each_user_tuple[3]
            original_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
            user_data = get_data_json_from_user_json(original_user_json)

            new_json = {}
            new_json['rank'] = str(i+1)
            print('rank: ' + new_json['rank'])
            new_json['name'] = str(user_data['username'])
            print('name: ' + new_json['name'])
            new_json['xp'] = str(user_data['skillXP'][skill_index])
            print('xp: ' + new_json['xp'])
            new_json['level'] = str(user_data['skillLevel'][skill_index])
            print('level: ' + new_json['level'])
            # Adding check for virtual levels since v0.15 of melvor
            if queryParam != 'total' and int(new_json['level']) >= 99:
                new_json['level'] = str(xp_to_virtual_level(new_json['xp']))
                print('virtual level: ' + new_json['level'])
            new_json['updt_dt_tm'] = str(original_user_json['updt_dt_tm']['S'])
            print('updt_dt_tm: ' + new_json['updt_dt_tm'])

            sorted_results.append(new_json)

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,GET'
            },
            'body': json.dumps(sorted_results)
        }
    except Exception as e:

        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,GET'
            },
            'body': str(e)
        }
