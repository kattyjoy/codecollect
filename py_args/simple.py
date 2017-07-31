import matplotlib.pyplot as plt
import numpy as np
import copy
import sys
import os

if __name__ == '__main__':
    assert len(sys.argv) == 2, 'usage: python graphone.py SQL_FOLDER'
    assert os.path.exists(sys.argv[1]), 'folder {0} not exist'.format(sys.argv[1])
    year_key = [
'2013.1',
 '2013.2',
 '2013.3',
 '2013.4',
 '2013.5',
'2013.6']
    data_series = {'2013.1': u'cluster and order by time',
                   '2013.2': u'cluster by position and order by time',
                   '2013.3': u'cluster by timeslot and sort by position grid id',
    '2013.4': 'l0_clean_ais_dynamic_2013_partby_position_byh',
    '2013.5': 'l0_clean_ais_dynamic_2013_partby_4hourtimeslot_sortby_h',
'2013.6': 'l0_clean_ais_dynamic_2013_partby_daytimeslot_sortby_h'
                   }
    bar_color_dict = {'2013.1': '#d9848c',
                      '2013.2': '#0079b5',
                      '2013.3': '#0aae8e',
                      '2013.4':'pink',
                       '2013.5':'yellow',
'2013.6':'blue'}
    plot_color_dict = {'2013.1': '#ef4860',
                       '2013.2': '#025078',
                       '2013.3': '#0c866e',
                      '2013.4':'red',
                       '2013.5':'orange',
'2013.6':'blue'}
    file_total = {
        '2013.1': 4544,
        '2013.2': 4545,
        '2013.3': 4545,
        '2013.4': 4545,
        '2013.5': 4545,
'2013.6':4545
    }
    # HOT TIME
    time_about_file_file = open('{0}/{0}-index-file.result'.format(sys.argv[1]))
    file_lines = time_about_file_file.readlines()
    time_about_file_file.close()
    file_title = file_lines[0].strip().split('\t')

    file_sql_index = next(i for i, x in enumerate(file_title) if 'sql' in x)
    file_count_index = next(i for i, x in enumerate(file_title) if 'result_file_count' in x)
    year_index = next(i for i, x in enumerate(file_title) if 'year' in x)
    file_result = [x.strip().split('\t') for x in file_lines[1:]]

    cost_dict = {}
    percent_dict = {}
    file_percent_dict = {}
    sql_where_dict = {}
    for key in year_key:
        file_percent_dict[key] = {}
        # test = {}
        # for sql,y in [[x[file_sql_index][x[file_sql_index].find('WHERE'):],
        #                            float(x[file_count_index])/ file_total[key]] for x in file_result if x[0] == key]:
        #     test.setdefault(sql,0)
        #     test[sql] += 1
        # print [(x,test[x]) for x in test if test[x] != 1]
        for sql, file_percent in [[x[file_sql_index][x[file_sql_index].find('WHERE'):],
                                   float(x[file_count_index]) / file_total[key]] for x in file_result if x[0] == key]:
            file_percent_dict[key][sql] = file_percent
    #print len(file_percent_dict[year_key[0]].keys())
    #print len(file_percent_dict[year_key[1]].keys())
    #print len(file_percent_dict[year_key[2]].keys())
    #print len(file_percent_dict[year_key[3]].keys())
    #print len(file_percent_dict[year_key[4]].keys())
    fig = plt.figure(figsize=(8, 4))
    ax = fig.add_subplot(111)
    bar_width = 0.25
    left_range = np.arange(len(file_percent_dict[year_key[0]]))*1.5
    offset = 0
    query_sql = file_percent_dict[year_key[0]].keys()
    for k in year_key:
        ax.bar(left_range + (bar_width * offset), [file_percent_dict[k][x] for x in query_sql],
               bar_width, color=bar_color_dict[k], label=data_series[k])
        # ax.plot(hive_bar.get_xaxis.get_ticklocs(), hive_average[year],'b--',label='Hive')
        # ax.plot(left_range + (bar_width * offset), cost_list, color=plot_color_dict[k], label=data_series[k], marker='s', markersize=5, linewidth=0.8)
        offset += 1
        #plt.xlim([-bar_width, len(hive_average[year])-bar_width])

    ax.set_title('Hot time query simulation')
    ax.legend()
    ax.set_xlabel('Query Case')
    ax.set_ylabel('data file hit proportion')
    # ax.set_xticklabels(['Q{0}'.format(x) for x in np.arange(1, len(query_sql) + 1)])

    plt.xticks(left_range + bar_width, np.arange(1, len(query_sql) + 1))
    # ax.set_xlim(left=-bar_width, right=bar_width*(len(year_key)+1)*len(query_sql))
    ax.set_ylim(bottom=0, top=1.2)

    plt.show()
