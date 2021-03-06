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

def calculate_combat_level(attack_lvl, strength_lvl, defence_lvl, hitpoints_lvl, ranged_lvl, magic_lvl, prayer_lvl):
    base = 0.25 * (int(defence_lvl) + int(hitpoints_lvl) + math.floor(int(prayer_lvl) / 2))
    melee = 0.325 * (int(attack_lvl) + int(strength_lvl))
    ranged = 0.325 * math.floor(3 * int(ranged_lvl) / 2)
    magic = 0.325 * math.floor(3 * int(magic_lvl) / 2)
    levels = [melee, ranged, magic]
    combat_level = math.floor(base + max(levels))
    return int(combat_level)

def calculate_num_99s(skills):
    return sum(1 for each_skill in skills if int(each_skill) >= 99)

def process_users_for_skill(queryParam, scan_result):
    users_tuple_list = []

    # for each user, put their user index in the bucket for their xp in the query param skill
    for each_user_index in range(0, len(scan_result['Items'])):
        each_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
        #print('each_user: ' + str(each_user_json))
        skill_index = skill_index_dict[queryParam]
        user_data = get_data_json_from_user_json(each_user_json)

        skill_xp = user_data['skillXP'][skill_index]
        skill_level = user_data['skillLevel'][skill_index]
        if queryParam != 'total' and queryParam != 'gp' and int(skill_level) >= 99:
            skill_level = xp_to_virtual_level(skill_xp)
            print('virtual level: ' + str(skill_level))

        name = user_data['username']

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
        new_json['level'] = str(user_data['skillLevel'][skill_index])
        print('level: ' + new_json['level'])
        new_json['xp'] = str(int(user_data['skillXP'][skill_index]))
        print('xp: ' + new_json['xp'])

        # Adding check for virtual levels since v0.15 of melvor
        if queryParam != 'total' and int(new_json['level']) >= 99:
            new_json['level'] = str(xp_to_virtual_level(new_json['xp']))
            print('virtual level: ' + new_json['level'])
        new_json['updt_dt_tm'] = str(original_user_json['updt_dt_tm']['S'])
        print('updt_dt_tm: ' + new_json['updt_dt_tm'])

        sorted_results.append(new_json)

    return sorted_results

def process_users_for_combat(queryParam, scan_result):
    users_tuple_list = []

    # for each user, put their user index in the bucket for their xp in the query param skill
    for each_user_index in range(0, len(scan_result['Items'])):
        each_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
        #print('each_user: ' + str(each_user_json))

        skills = ['attack', 'strength', 'defence', 'hitpoints', 'ranged', 'magic', 'prayer']
        skill_lvls = []
        for each_skill in skills:
            skill_index = skill_index_dict[each_skill]
            user_data = get_data_json_from_user_json(each_user_json)

            skill_level = user_data['skillLevel'][skill_index]
            skill_lvls.append(skill_level)

        name = user_data['username']

        combat_level = calculate_combat_level(skill_lvls[0], skill_lvls[1], skill_lvls[2], skill_lvls[3], skill_lvls[4], skill_lvls[5], skill_lvls[6])

        each_user_tuple = (combat_level, name, each_user_index)
        users_tuple_list.append(each_user_tuple)

    print(str(users_tuple_list))

    s = sorted(users_tuple_list, key = lambda x: (x[0], x[1]), reverse=True)

    sorted_results = []

    for i, each_user_tuple in enumerate(s):
        each_user_index = each_user_tuple[2]
        original_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
        user_data = get_data_json_from_user_json(original_user_json)

        new_json = {}
        new_json['rank'] = str(i+1)
        print('rank: ' + new_json['rank'])
        new_json['name'] = str(user_data['username'])
        print('name: ' + new_json['name'])
        new_json['level'] = str(calculate_combat_level(user_data['skillLevel'][skill_index_dict['attack']], \
            user_data['skillLevel'][skill_index_dict['strength']], user_data['skillLevel'][skill_index_dict['defence']], \
            user_data['skillLevel'][skill_index_dict['hitpoints']], user_data['skillLevel'][skill_index_dict['ranged']], \
            user_data['skillLevel'][skill_index_dict['magic']], user_data['skillLevel'][skill_index_dict['prayer']]))
        print('level: ' + new_json['level'])

        new_json['updt_dt_tm'] = str(original_user_json['updt_dt_tm']['S'])
        print('updt_dt_tm: ' + new_json['updt_dt_tm'])

        sorted_results.append(new_json)

    return sorted_results


def process_users_for_gp(queryParam, scan_result):
    users_tuple_list = []

    # for each user, put their user index in the bucket for their xp in the query param skill
    for each_user_index in range(0, len(scan_result['Items'])):
        each_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
        #print('each_user: ' + str(each_user_json))
        user_data = get_data_json_from_user_json(each_user_json)

        # exclude the total level
        user_data['num99s'] = calculate_num_99s(user_data['skillLevel'][1:]) if 'skillLevel' in user_data.keys() else 0

        gp = user_data['gp'] if 'gp' in user_data.keys() else 0
        num99s = user_data['num99s'] if 'num99s' in user_data.keys() else 0
        pets = user_data['pets'] if 'pets' in user_data.keys() else 0

        name = user_data['username']

        each_user_tuple = (gp, num99s, pets, name, each_user_index)
        users_tuple_list.append(each_user_tuple)

    print(str(users_tuple_list))

    s = sorted(users_tuple_list, key = lambda x: (x[0], x[1], x[2], x[3]), reverse=True)

    sorted_results = []

    for i, each_user_tuple in enumerate(s):
        each_user_index = each_user_tuple[-1]
        original_user_json = get_user_json_from_scan_for_index(scan_result, each_user_index)
        user_data = get_data_json_from_user_json(original_user_json)

        # exclude the total level
        user_data['num99s'] = calculate_num_99s(user_data['skillLevel'][1:]) if 'skillLevel' in user_data.keys() else 0

        new_json = {}
        new_json['rank'] = str(i+1)
        print('rank: ' + new_json['rank'])
        new_json['name'] = str(user_data['username'])
        print('name: ' + new_json['name'])
        new_json['gp'] = str(user_data['gp'] if 'gp' in user_data.keys() else 0)
        print('gp: ' + new_json['gp'])
        new_json['num99s'] = str(user_data['num99s'] if 'num99s' in user_data.keys() else 0)
        print('num99s: ' + new_json['num99s'])
        new_json['pets'] = str(user_data['pets'] if 'pets' in user_data.keys() else 0)
        print('pets: ' + new_json['pets'])
        new_json['updt_dt_tm'] = str(original_user_json['updt_dt_tm']['S'])
        print('updt_dt_tm: ' + new_json['updt_dt_tm'])

        sorted_results.append(new_json)

    return sorted_results

def lambda_handler(event, context):

    try:

        queryParam = extract_skill(event).lower()
        print('queryParam: ' + queryParam)

        scan_result = client.scan(
            TableName='MelvorHiscores'
        )

        if queryParam == 'gp':
            sorted_results = process_users_for_gp(queryParam, scan_result)
        elif queryParam == 'combat':
            sorted_results = process_users_for_combat(queryParam, scan_result)
        else:
            sorted_results = process_users_for_skill(queryParam, scan_result)

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
