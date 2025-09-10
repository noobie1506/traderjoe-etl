import os
import json
import requests
import pandas as pd
from datetime import datetime
from web3 import Web3
import time
from config import user_addresses, pool_addresses, tj_dex_key, start_time_str, end_time_str

def main():
    for user_address in user_addresses:
        for pool_address in pool_addresses:
            try:
                print(f"Processing data for user: {user_address} and pool: {pool_address}")
                process_data(user_address, pool_address)
            except Exception as e:
                print(f"An error occurred for user {user_address} and pool {pool_address}: {e}")


def process_data(user_address, pool_address):
    chain = "avalanche"
    headers = {'x-traderjoe-api-key': tj_dex_key}

    start_time_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    end_time_dt = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")

    start_time_unix = int(start_time_dt.timestamp())
    end_time_unix = int(end_time_dt.timestamp())

    params = {
        'pageSize': 100,
        'startTime': start_time_unix,
        'endTime': end_time_unix
    }

    def get_api_data(url, additional_params=None):
        request_params = params.copy()
        if additional_params:
            request_params.update(additional_params)
        response = requests.get(url, headers=headers, params=request_params)
        response.raise_for_status()
        return response.json()

    url_pool_data = f"https://api.lfj.dev/v1/pools/avalanche/{pool_address}"

    filter_by = '1d'
    pool_params = {
        'filterBy': filter_by
    }

    pool_data = get_api_data(url_pool_data, additional_params=pool_params)
    df_pools = pd.json_normalize(pool_data)

    columns_to_keep = {
        'pairAddress': 'pool_address',
        'name': 'pool_name',
        'volumeUsd': f'pool[volume]({filter_by})',
        'liquidityUsd': 'pool[liquidity]',
        'feesUsd': f'pool[total_fees(USD)]({filter_by})',
        'tokenX.address': 'token_x_address',
        'tokenY.address': 'token_y_address',
        'tokenX.symbol': 'token_x_symbol',
        'tokenY.symbol': 'token_y_symbol',
        'tokenX.decimals': 'token_x_decimals',
        'tokenY.decimals': 'token_y_decimals',
        'reserveX': 'pool[token_x_amount]',
        'reserveY': 'pool[token_y_amount]',
        'tokenX.priceUsd': 'token_x_price',
        'tokenY.priceUsd': 'token_y_price',
        'lbBinStep': 'lbBinStep',
        'lbBaseFeePct': 'base_fee%',
        'lbMaxFeePct': 'max_fee%',
        'protocolSharePct': 'protocol_fee%',
        'activeBinId': 'activeBinId',
        'liquidityDepthMinus': 'liquidityDepth-2%(USD)',
        'liquidityDepthPlus': 'liquidityDepth+2%(USD)',
        'liquidityDepthTokenX': 'liquidityDepth+2%TokenX',
        'liquidityDepthTokenY': 'liquidityDepth-2%TokenY'
    }

    existing_columns = {col: columns_to_keep[col] for col in columns_to_keep if col in df_pools.columns}
    df_pools = df_pools[list(existing_columns.keys())]
    df_pools.rename(columns=existing_columns, inplace=True)

    filter_by_1h = '1h'
    pool_params_1h = {
        'filterBy': filter_by_1h
    }

    pool_data_1h = get_api_data(url_pool_data, additional_params=pool_params_1h)
    df_pools_1h = pd.json_normalize(pool_data_1h)

    columns_to_keep_1h = {
        'volumeUsd': f'pool[volume]({filter_by_1h})',
        'feesUsd': f'pool[total_fees(USD)]({filter_by_1h})'
    }

    existing_columns_1h = {col: columns_to_keep_1h[col] for col in columns_to_keep_1h if col in df_pools_1h.columns}
    df_pools_1h = df_pools_1h[list(existing_columns_1h.keys())]
    df_pools_1h.rename(columns=columns_to_keep_1h, inplace=True)

    df_pools = pd.concat([df_pools, df_pools_1h], axis=1)

    url_lfj_history = f"https://api.lfj.dev/v1/user/{chain}/history/{user_address}/{pool_address}"
    lfj_history_data = get_api_data(url_lfj_history)

    extracted_data = []
    for entry in lfj_history_data:
        data_point = {
            'timestamp': entry.get('timestamp'),
            'isDeposit': entry.get('isDeposit'),
            'poolAddress': entry.get('poolAddress'),
            'pairName': entry.get('pairName'),
            'binId': entry.get('binId'),
            'tokenX_amount': float(entry.get('tokenX', {}).get('amount', 0)),
            'tokenX_price': float(entry.get('tokenX', {}).get('price', 0)),
            'tokenY_amount': float(entry.get('tokenY', {}).get('amount', 0)),
            'tokenY_price': float(entry.get('tokenY', {}).get('price', 0)),
            'blockNumber': entry.get('blockNumber')
        }
        extracted_data.append(data_point)

    df_lfj_history = pd.DataFrame(extracted_data)
    df_lfj_history['timestamp'] = pd.to_datetime(df_lfj_history['timestamp'])
    df_lfj_history['tokenX_amount'] = pd.to_numeric(df_lfj_history['tokenX_amount'], errors='coerce').fillna(0)
    df_lfj_history['tokenY_amount'] = pd.to_numeric(df_lfj_history['tokenY_amount'], errors='coerce').fillna(0)
    df_deposits = df_lfj_history[df_lfj_history['isDeposit'] == True]

    if not df_deposits.empty:
        most_recent_block_number = df_deposits['blockNumber'].max()
        most_recent_deposits = df_deposits[df_deposits['blockNumber'] == most_recent_block_number]
        most_recent_timestamp = most_recent_deposits['timestamp'].max()
        most_recent_deposits = most_recent_deposits[most_recent_deposits['timestamp'] == most_recent_timestamp]
        total_tokenX_amount = most_recent_deposits['tokenX_amount'].sum()
        total_tokenY_amount = most_recent_deposits['tokenY_amount'].sum()
        most_recent_deposit_time = most_recent_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        df_history = pd.DataFrame([{
            'pool_name': df_pools['pool_name'].iloc[0],
            'total_tokenX_amount_initial_deposit': total_tokenX_amount,
            'total_tokenY_amount_initial_deposit': total_tokenY_amount,
            'MostRecentDepositTime': most_recent_deposit_time
        }])
    else:
        print("No deposit entries found.")
        total_tokenX_amount = 0
        total_tokenY_amount = 0
        most_recent_deposit_time = None
        df_history = pd.DataFrame([{
            'pool_name': df_pools['pool_name'].iloc[0],
            'total_tokenX_amount_initial_deposit': total_tokenX_amount,
            'total_tokenY_amount_initial_deposit': total_tokenY_amount,
            'MostRecentDepositTime': most_recent_deposit_time
        }])

    url_fees_earned = f"https://api.traderjoexyz.dev/v1/user/fees-earned/{chain}/{user_address}/{pool_address}"
    fees_data = get_api_data(url_fees_earned)

    formatted_fees_data = []
    total_accrued_fees_x = 0
    total_accrued_fees_y = 0

    for entry in fees_data:
        bin_id = str(entry.get('binId'))
        accrued_fees_x = float(entry.get('accruedFeesX', 0))
        accrued_fees_y = float(entry.get('accruedFeesY', 0))
        total_accrued_fees_x += accrued_fees_x
        total_accrued_fees_y += accrued_fees_y
        formatted_fees_data.append(f"{bin_id}: {accrued_fees_x}, {accrued_fees_y}")

    combined_fees = "(" + "; ".join(formatted_fees_data) + ")"
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    fees_data_dict = [{
        'pool_name': df_pools['pool_name'].iloc[0],
        'timestamp(datetime_pst)': current_time,
        'fees_per_bin(bin_id: token_x, token_y_amounts)': combined_fees,
        'accrued_fees_token_x': total_accrued_fees_x,
        'accrued_fees_token_y': total_accrued_fees_y
    }]

    df_fees = pd.DataFrame(fees_data_dict)
    merged_df = pd.merge(df_pools, df_history, on='pool_name', how='left')
    merged_df = pd.merge(merged_df, df_fees, on='pool_name', how='left')

    numeric_columns = [
        'total_tokenX_amount_initial_deposit',
        'total_tokenY_amount_initial_deposit',
        'token_x_price',
        'token_y_price',
        'accrued_fees_token_x',
        'accrued_fees_token_y',
        'pool[liquidity]',
        f'pool[total_fees(USD)]({filter_by})'
    ]
    for col in numeric_columns:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    merged_df['value_if_held(USD)'] = (
            merged_df['total_tokenX_amount_initial_deposit'] * merged_df['token_x_price'] +
            merged_df['total_tokenY_amount_initial_deposit'] * merged_df['token_y_price']
    )

    w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))
    assert w3.is_connected(), "Failed to connect to Avalanche C-Chain."

    contract_address = Web3.to_checksum_address('0xA5c68C9E55Dde3505e60c4B5eAe411e2977dfB35')
    with open('helperContractABI.json', 'r') as f:
        abi = json.load(f)
    contract = w3.eth.contract(address=contract_address, abi=abi)

    lb_pair_address = Web3.to_checksum_address(pool_address)
    active_bin_id = int(merged_df['activeBinId'].iloc[0])
    ids_plus = 1000
    ids_minus = 1000

    result = contract.functions.getBinsReserveOf(
        lb_pair_address, user_address, active_bin_id, ids_plus, ids_minus
    ).call()
    user_data = result[1]

    merged_df['token_x_decimals'] = pd.to_numeric(merged_df['token_x_decimals'], errors='coerce')
    merged_df['token_y_decimals'] = pd.to_numeric(merged_df['token_y_decimals'], errors='coerce')

    token_x_decimals = int(merged_df['token_x_decimals'].iloc[0])
    token_y_decimals = int(merged_df['token_y_decimals'].iloc[0])

    user_tokenX_raw_total = 0
    user_tokenY_raw_total = 0
    bin_strings = []

    for bin_id, reserveX, reserveY, shares, total_shares in user_data:
        tokenX_reserves = reserveX * shares / total_shares
        tokenY_reserves = reserveY * shares / total_shares
        user_tokenX_raw_total += tokenX_reserves
        user_tokenY_raw_total += tokenY_reserves

        tokenX_reserves_formatted = tokenX_reserves / (10 ** token_x_decimals)
        tokenY_reserves_formatted = tokenY_reserves / (10 ** token_y_decimals)
        bin_strings.append(f"{bin_id}: {tokenX_reserves_formatted}, {tokenY_reserves_formatted}")

    user_tokenX_contract = user_tokenX_raw_total / (10 ** token_x_decimals)
    user_tokenY_contract = user_tokenY_raw_total / (10 ** token_y_decimals)

    bins_data = "(" + "; ".join(bin_strings) + ")"

    df_contract = pd.DataFrame([{
        'bin_distribution(bin id: token_x_amount, token_y_amounts)': bins_data,
        'token_x_amount': user_tokenX_contract,
        'token_y_amount': user_tokenY_contract
    }])

    merged_df = pd.concat([merged_df, df_contract], axis=1)

    merged_df['token_x_amount'] = pd.to_numeric(merged_df['token_x_amount'], errors='coerce')
    merged_df['token_y_amount'] = pd.to_numeric(merged_df['token_y_amount'], errors='coerce')

    merged_df['token_x(USD)'] = merged_df['token_x_price'] * merged_df['token_x_amount']
    merged_df['token_y(USD)'] = merged_df['token_y_price'] * merged_df['token_y_amount']
    merged_df['total_token_value(USD)'] = merged_df['token_x(USD)'] + merged_df['token_y(USD)']

    merged_df['accrued_fees_token_x(USD)'] = merged_df['accrued_fees_token_x'] * merged_df['token_x_price']
    merged_df['accrued_fees_token_y(USD)'] = merged_df['accrued_fees_token_y'] * merged_df['token_y_price']

    merged_df['impermanent_loss(USD)'] = merged_df['value_if_held(USD)'] - merged_df['total_token_value(USD)']

    merged_df['pool[liquidity]'] = pd.to_numeric(merged_df['pool[liquidity]'], errors='coerce')
    merged_df['user_%_of_pool_liquidity'] = (merged_df['total_token_value(USD)'] / merged_df['pool[liquidity]']) * 100
    merged_df['user_%_of_pool_liquidity'] = merged_df['user_%_of_pool_liquidity'].replace([float('inf'), -float('inf')],
                                                                                          float('nan'))

    current_unix_time = int(time.time())
    merged_df.insert(0, 'current_unix_timestamp', current_unix_time)
    merged_df.insert(1, 'user_address', user_address)

    merged_df[f'pool[total_fees(USD)]({filter_by})'] = pd.to_numeric(merged_df[f'pool[total_fees(USD)]({filter_by})'],
                                                                     errors='coerce')

    merged_df['fees_annual'] = merged_df[f'pool[total_fees(USD)]({filter_by})'] * 365
    merged_df['APR%'] = (merged_df['fees_annual'] / merged_df['pool[liquidity]']) * 100
    merged_df['APY%'] = ((1 + (
                merged_df[f'pool[total_fees(USD)]({filter_by})'] / merged_df['pool[liquidity]'])) ** 365 - 1) * 100
    merged_df['APR_1d%'] = (merged_df[f'pool[total_fees(USD)]({filter_by})'] / merged_df['pool[liquidity]']) * 100

    merged_df['APR%'] = merged_df['APR%'].replace([float('inf'), -float('inf')], float('nan'))
    merged_df['APY%'] = merged_df['APY%'].replace([float('inf'), -float('inf')], float('nan'))
    merged_df['APR_1d%'] = merged_df['APR_1d%'].replace([float('inf'), -float('inf')], float('nan'))

    merged_df[f'pool[volume]({filter_by_1h})'] = pd.to_numeric(merged_df[f'pool[volume]({filter_by_1h})'],
                                                               errors='coerce')
    merged_df[f'pool[total_fees(USD)]({filter_by_1h})'] = pd.to_numeric(
        merged_df[f'pool[total_fees(USD)]({filter_by_1h})'], errors='coerce')

    desired_columns_order = [
        'current_unix_timestamp',
        'timestamp(datetime_pst)',
        'pool_name',
        'pool_address',
        f'pool[volume]({filter_by_1h})',
        'pool[liquidity]',
        f'pool[total_fees(USD)]({filter_by_1h})',
        'lbBinStep',
        'base_fee%',
        'max_fee%',
        'protocol_fee%',
        'token_x_symbol',
        'token_y_symbol',
        'token_x_address',
        'token_y_address',
        'pool[token_x_amount]',
        'pool[token_y_amount]',
        'token_x_price',
        'token_y_price',
        'activeBinId',
        'liquidityDepth+2%TokenX',
        'liquidityDepth-2%TokenY',
        'liquidityDepth+2%(USD)',
        'liquidityDepth-2%(USD)',
        'user_address',
        'total_tokenX_amount_initial_deposit',
        'total_tokenY_amount_initial_deposit',
        'MostRecentDepositTime',
        'token_x_amount',
        'token_y_amount',
        'token_x(USD)',
        'token_y(USD)',
        'bin_distribution(bin id: token_x_amount, token_y_amounts)',
        'total_token_value(USD)',
        'accrued_fees_token_x',
        'accrued_fees_token_y',
        'accrued_fees_token_x(USD)',
        'accrued_fees_token_y(USD)',
        'fees_per_bin(bin_id: token_x, token_y_amounts)',
        'value_if_held(USD)',
        'impermanent_loss(USD)',
        'user_%_of_pool_liquidity',
        'fees_annual',
        'APR%',
        'APY%',
        'APR_1d%'
    ]

    existing_columns_order = [col for col in desired_columns_order if col in merged_df.columns]
    merged_df = merged_df[existing_columns_order]

    output_csv_file = '/Users/jackwho/Desktop/merged_data.csv'
    if os.path.isfile(output_csv_file) and os.path.getsize(output_csv_file) > 0:
        merged_df.to_csv(output_csv_file, mode='a', index=False, header=False)
        print(f"Data successfully appended to {output_csv_file}")
    else:
        merged_df.to_csv(output_csv_file, index=False)
        print(f"Data successfully saved to {output_csv_file}")

if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
        time.sleep(3600)