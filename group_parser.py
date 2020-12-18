# ChickenFryBiryani

import os
import json
import datetime
import database_connector
from pathlib import Path

telegram_data_folder_path = "/home/rob/Downloads/Telegram Desktop/"
chat_folder_path = ""
remote_telegram_root = "/home/telegram/"
m_sServerPass = "MdWbyTiW2TQGNfGx4MdJ"
local_path = chat_folder_path
m_sServerUser = "telegram"
m_sServerHost = "jaguar.cs.gsu.edu"
telegram_group_id = ""


def getDateString(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').strftime('%Y%m%d%H%M%S')


def getMediaPath(message):
    if 'photo' in message:
        return str(telegram_group_id) + '/' + chat_folder_path.split('/')[-2].split('_')[1] + '/' + message['photo']
    if 'file' in message:
        return str(telegram_group_id) + '/' + chat_folder_path.split('/')[-2].split('_')[1] + '/' + message['file']
    return ''


def getText(message):
    if 'text' not in message or message['text'] == '':
        return ''
    if type(message['text']) == str:
        return message['text']
    return ' '.join(list(map(lambda x: x.strip() if type(x) == str else x['text'].strip(), message['text'])))


def main():
    global telegram_group_id
    global chat_folder_path
    # all_folders = os.listdir(telegram_data_folder_path)
    # all_folders.sort()
    all_folders = list(map(lambda x: str(x), sorted(Path(telegram_data_folder_path).iterdir(), key=os.path.getmtime)))
    for folder in all_folders:
        print('***************************************************************')
        print('Parsing: ', folder)
        chat_folder_path = folder + '/'
        with open(chat_folder_path + 'result.json') as chat_file:
            chat_content = json.load(chat_file)
        if 'group' not in chat_content['type']:
            print('Not a Group. Channel parser will pick it up.')
            continue
        telegram_group_id = chat_content['id']
        telegram_group_name = chat_content['name'].replace("'", '"')
        telegram_db = database_connector.mySQLTelegramDB()
        search_response = telegram_db.get_group_id_from_telegram_id(telegram_group_id)
        if not search_response:
            continue
        if search_response > 0:
            group_id = search_response
        else:
            add_response = telegram_db.add_group(telegram_group_id, telegram_group_name)
            if not add_response:
                continue
            group_id = add_response
        from_msg_id = telegram_db.get_last_added_msg_id_in_group(group_id)
        if not from_msg_id:
            continue
        pending_msgs = list(filter(lambda x: x['id'] > from_msg_id, chat_content['messages']))
        if len(pending_msgs) <= 1:
            print('No new messages.')
            # if input('Delete the data in local system?(y/n): ').lower() == 'y':
            os.system('rm -rf {}'.format(chat_folder_path.replace(' ', '\ ').replace('(', '\(').replace(')', '\)')))
            continue
        # Add all the new users to group_users table
        users = list(set(map(lambda x: (str(x['from']).replace("'", "").replace('(', '').replace(')', ''), x['from_id']) if 'from' in x else (str(x['actor']).replace("'", "").replace('(', '').replace(')', ''), x['actor_id']),
                             pending_msgs)))
        user_dict = {}
        for use in users:
            if use[1] in user_dict:
                if use[0] != 'None':
                    user_dict[use[1]] = use[0]
            else:
                user_dict[use[1]] = use[0]
        users = list(map(lambda x: (user_dict[x], x), user_dict))
        telegram_ids = list(map(lambda x: x[1], users))
        user_ids = telegram_db.add_users_if_not_exists(users)
        new_msgs = list(map(lambda x: (group_id, 'message' if x['type'] == 'message' else x['action'], str(x['id']),
                                       user_ids[telegram_ids.index(x['from_id'] if 'from_id' in x else x['actor_id'])],
                                       x['reply_to_message_id'] if 'reply_to_message_id' in x else '',
                                       getDateString(x['date']), getText(x), getMediaPath(x)), pending_msgs))
        rows_added = 0
        for batch_idx in range(0, len(new_msgs), 10000):
            batch = new_msgs[batch_idx: min(len(new_msgs), batch_idx+10000)]
            rows_added += telegram_db.add_group_messages(batch)
        print('Chat posts added: ', rows_added)
        # Copy telegram chat to jaguar
        remote_path = str(telegram_group_id) + '/'
        # Rename the result json
        os.rename(chat_folder_path + 'result.json', chat_folder_path + "{}.json".
                  format(chat_folder_path.split('_')[-1].replace(' ', '_')[:-1]))
        telegram_db.copy_folder_to_jaguar(chat_folder_path, remote_path, is_group=True)
        # if input('Delete the data in local system?(y/n): ').lower() == 'y':
        os.system('rm -rf {}'.format(chat_folder_path.replace(' ', '\ ').replace('(', '\(').replace(')', '\)')))
    return


if __name__ == '__main__':
    main()
