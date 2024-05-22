import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
import seaborn as sns

GRAPH_DIR="experiment/plot/"
def SetupMplParams(set):
    #color = sns.color_palette("deep", 7)
    color = sns.color_palette(set, n_colors=12, desat=0.7)
    mpl.rcParams.update(mpl.rcParamsDefault)

    # mpl.rcParams['ps.useafm'] = True
    # mpl.rcParams['pdf.use14corefonts'] = True
    # mpl.rcParams['text.usetex'] = True
    # mpl.rcParams['text.latex.preamble'] = [
    #         #r'\usepackage{siunitx}',   # i need upright \micro symbols, but you need...
    #         #r'\sisetup{detect-all}',   # ...this to force siunitx to actually use your fonts
    #         r'\usepackage{helvet}',    # set the normal font here
    #         r'\usepackage{sansmath}',  # load up the sansmath so that math -> helvet
    #         r'\sansmath' # <- tricky! -- # gotta actually # tell tex to use!
    #         ]
    mpl.rcParams['xtick.labelsize'] = 12
    mpl.rcParams['ytick.labelsize'] = 12

    return color

def autolabel(ax, rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., height + 0.01,
                '%.f' % height, size=10,
                ha='center', va='bottom')
# 绘制折线图(Time Cost)
# xaxis:['customer','lineitem',...]
# data Cost
# name：文件名
# xlabel：x轴名称
# ylabel：y轴名称
# color：线条颜色
def PlotLineChart(filename1,filename2, color):
    BASE_PATH = "result/"
    data = pd.read_csv(BASE_PATH + filename1, header=None)
    xaxis =np.around(np.array(data.iloc[0, 1:].astype('float')),decimals=2)

    labels=['Huang-'+label for label in np.array(data.iloc[1:, 0])]
    result=np.array(data.iloc[1:,1:].astype('float'))
    fig = plt.figure()
    fig.set_size_inches(12, 5)
    ax = fig.add_subplot(111)
    loc = plticker.MultipleLocator(1) # this locator puts ticks at regular intervals
    ax.xaxis.set_major_locator(loc)
    loc = plticker.MaxNLocator(5) # this locator puts ticks at regular intervals
    ax.yaxis.set_major_locator(loc)
    ax.grid()
    n = len(xaxis)
    ax.set_ylabel("Estimate Cost (block)",fontsize=15)
    # ax.set_xlabel("Cost Model:%s,BenchMark:%s"%(costmodel,benchmark),fontsize=15,weight='bold')
    #ax.set_xlim([xaxis[0], xaxis[-1]])

    marker=['o','^','s','*','v',]
    for index,label in enumerate(labels):
        ax.plot(range(n),result[index], marker = marker[index%3], color = color[index], label = label, linewidth = 1.5)
        # ax.plot(range(n), data[1][:n], marker = '^', color = color[3], label = 'BusTracker', linewidth = 3)
        # ax.plot(range(n), data[2][:n], marker = 's', color = color[0], label = 'MOOC', linewidth = 3)
    #ax.legend(bbox_to_anchor = [1, 0.3], loc = 'lower right')
    ax.legend(bbox_to_anchor=[0.2, 1], loc='lower center', ncol=len(labels))

    data = pd.read_csv(BASE_PATH + filename2, header=None)
    labels =['Son-'+label for label in np.array(data.iloc[1:, 0])]
    result = np.array(data.iloc[1:, 1:].astype('float'))
    ax2 = ax.twinx()
    for index,label in enumerate(labels):
        ax2.plot(range(n),result[index], marker = marker[index+2], color = color[index+2], label = label, linewidth = 1.5)
    ax2.set_ylabel("System Load", fontsize=15)
    # ax2.set_ylim([-0.5,4])
    ax2.set_ylim([20,60])
    ax2.legend(bbox_to_anchor=[0.7, 1], loc='lower center', ncol=len(labels))
    # xtick_index=[i*2 for i in range(int(n/2))]
    ax2.set_xticks(range(n))
    ax2.set_xticklabels(xaxis)
    ax2.set_xlabel("min-support", fontsize=15)
    # plt.show()
    plt.savefig("%s/scvp-random-minsup-sensitivity.png" % (GRAPH_DIR), bbox_inches='tight')
    plt.close(fig)

# 绘制简单的柱状图
def PlotSingleBar(filename,costmodel,benchmark,ylabel):
    # colors=SetupMplParams('tab20c')
    colors=SetupMplParams('tab20c')
    BASE_PATH = "result/"
    data = pd.read_csv(BASE_PATH + filename, header=None)
    # 分成两个图画
    # 方法名
    labels = np.array(data.iloc[0, 1:])
    result=np.array(data.iloc[1, 1:].astype('float'))
    fig_length = 15
    fig = plt.figure()
    fig.set_size_inches(fig_length, 8)
    ax = fig.add_subplot(1, 1, 1)
    loc = plticker.MaxNLocator(3)  # this locator puts ticks at regular intervals
    ax.yaxis.set_major_locator(loc)
    # xticks=np.arange(len(labels))
    # ax.set_xticks(xticks)
    # ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel, fontsize=15)
    ax.set_xlabel("Cost Model:%s,BenchMark:%s"%(costmodel,benchmark), fontsize=15,weight='bold')
    # print(result)
    bars=ax.bar(x=labels,height=result,width=0.7)
    for bar, color in zip(bars, colors):  # 给每个bar分配指定的颜色
        bar.set_color(color)
    rects = ax.patches
    autolabel(ax, rects)
    # plt.show()
    plt.savefig("%s/%s-%s-%s.png" % (GRAPH_DIR, costmodel, benchmark,ylabel), bbox_inches='tight')
# 绘制柱形图（Son Cost）
def PlotPaperGraph(filename,costmodel,benchmark,color):

    BASE_PATH="result/"
    data=pd.read_csv(BASE_PATH+filename,header=None)
    # 分成两个图画
    # 方法名
    labels=np.array(data.iloc[0,1:])

    complete_xlabels_length=len(data.iloc[1:,0])
    fig_length = 18
    fig = plt.figure()
    fig.set_size_inches(fig_length, 10)
    if(benchmark=='tpch'):
        xlabels_stage_length=int(complete_xlabels_length / 2)
        for i in range(2):
            # 数据集
            start_row=1+i*xlabels_stage_length
            end_row=start_row+xlabels_stage_length
            xlabels = np.array(data.iloc[start_row:end_row, 0])
            # result=np.array(data.iloc[1:,1:]).transpose()
            result=np.array(data.iloc[start_row:end_row,1:].astype('float')).transpose()
            ax = fig.add_subplot(2,1,i+1)
            loc = plticker.MaxNLocator(3)  # this locator puts ticks at regular intervals
            ax.yaxis.set_major_locator(loc)
            ax.grid()
            if costmodel=='huang':
                ax.set_ylabel("Estimate Cost (block)", fontsize=15)
            else:
                ax.set_ylabel("System Load", fontsize=15)
            if i==1:
                ax.set_xlabel("Cost Model:%s,BenchMark:%s"%(costmodel,benchmark), fontsize=15, weight='bold')
            bar_width=0.8
            x = [x * 15 for x in range(xlabels_stage_length)]
            ax.bar([i - 1 for i in x], result[0], bar_width, label=labels[0], hatch='\\', color=color[0])
            ax.bar([i for i in x], result[1], bar_width, label=labels[1], hatch='-', color=color[1])
            ax.bar([i + 1 for i in x], result[2], bar_width, label=labels[2], hatch='|', color=color[2])
            ax.bar([i + 2 for i in x], result[3], bar_width, label=labels[3], hatch='\\', color=color[3])
            ax.bar([i + 3 for i in x], result[4], bar_width, label=labels[4], hatch='-', color=color[4])
            ax.bar([i + 4 for i in x], result[5], bar_width, label=labels[5], hatch='|', color=color[5])
            ax.bar([i + 5 for i in x], result[6], bar_width, label=labels[6], hatch='\\', color=color[6])
            ax.bar([i + 6 for i in x], result[7], bar_width, label=labels[7], hatch='-', color=color[7])
            ax.bar([i + 7 for i in x], result[8], bar_width, label=labels[8], hatch='|', color=color[8])
            ax.bar([i + 8 for i in x], result[9], bar_width, label=labels[9], hatch='\\', color=color[9])
            ax.bar([i + 9 for i in x], result[10], bar_width, label=labels[10], hatch='-', color=color[10])
            # ax.bar([i + 10 for i in x], result[11], bar_width, label=labels[11], hatch='|', color=color[11])
            if(costmodel=='son'):
                rects = ax.patches
                autolabel(ax, rects)
            ax.set_xticks([i + xlabels_stage_length for i in x])
            # ax.set_xlim([-2, x[-1] + 12])
            ax.set_xticklabels(xlabels, fontsize=15)
            if i==0:
                ax.legend(bbox_to_anchor=[0.5, 1], loc='lower center', ncol=len(labels))
    else:
        # 数据集
        xlabels = np.array(data.iloc[1:, 0])
        result = np.array(data.iloc[1:, 1:].astype('float')).transpose()
        ax = fig.add_subplot(1, 1, 1)
        loc = plticker.MaxNLocator(3)  # this locator puts ticks at regular intervals
        ax.yaxis.set_major_locator(loc)
        ax.grid()
        if costmodel == 'huang':
            ax.set_ylabel("Estimate Cost (block)", fontsize=15)
        else:
            ax.set_ylabel("System Load", fontsize=15)

        ax.set_xlabel("Cost Model:%s,BenchMark:%s" % (costmodel, benchmark), fontsize=15, weight='bold')
        bar_width = 0.8
        x = [x * 15 for x in range(complete_xlabels_length)]
        ax.bar([i - 1 for i in x], result[0], bar_width, label=labels[0], hatch='\\', color=color[0])
        ax.bar([i for i in x], result[1], bar_width, label=labels[1], hatch='-', color=color[1])
        ax.bar([i + 1 for i in x], result[2], bar_width, label=labels[2], hatch='|', color=color[2])
        ax.bar([i + 2 for i in x], result[3], bar_width, label=labels[3], hatch='\\', color=color[3])
        ax.bar([i + 3 for i in x], result[4], bar_width, label=labels[4], hatch='-', color=color[4])
        ax.bar([i + 4 for i in x], result[5], bar_width, label=labels[5], hatch='|', color=color[5])
        ax.bar([i + 5 for i in x], result[6], bar_width, label=labels[6], hatch='\\', color=color[6])
        ax.bar([i + 6 for i in x], result[7], bar_width, label=labels[7], hatch='-', color=color[7])
        ax.bar([i + 7 for i in x], result[8], bar_width, label=labels[8], hatch='|', color=color[8])
        ax.bar([i + 8 for i in x], result[9], bar_width, label=labels[9], hatch='\\', color=color[9])
        ax.bar([i + 9 for i in x], result[10], bar_width, label=labels[10], hatch='-', color=color[10])
        # ax.bar([i + 10 for i in x], result[11], bar_width, label=labels[11], hatch='|', color=color[11])
        if (costmodel == 'son'):
            rects = ax.patches
            autolabel(ax, rects)
        ax.set_xticks([i + complete_xlabels_length for i in x])
        # ax.set_xlim([-2, x[-1] + 12])
        ax.set_xticklabels(xlabels, fontsize=15)
        ax.legend(bbox_to_anchor=[0.5, 1.02], loc='lower center', ncol=len(labels))

    plt.savefig("%s/%s-%s-compare.png" % (GRAPH_DIR, costmodel,benchmark), bbox_inches='tight')
    plt.close(fig)
def Main():
    costmodel = ['huang', 'son']
    benchmark = ['tpch', 'ssb','randomAttr','randomQue']
    # 绘制Son Cost和 Huang Cost on tpch 和ssb

    # color=SetupMplParams("Paired")
    # PlotPaperGraph('tpch-Huang-cost-result.csv', costmodel[0], benchmark[0], color)
    # PlotPaperGraph('ssb-Huang-cost-result.csv', costmodel[0], benchmark[1], color)
    # PlotPaperGraph('tpch-Son-cost-result.csv',costmodel[1],benchmark[0],color)
    # PlotPaperGraph('ssb-Son-cost-result.csv',costmodel[1],benchmark[1],color)


    # 绘制Son Cost和 Huang Cost on random dataset

    color = SetupMplParams("Set2")
    # PlotPaperGraph('randomAttr-Huang-cost-result.csv', costmodel[0], benchmark[2], color)
    # PlotPaperGraph('randomQue-Huang-cost-result.csv', costmodel[0], benchmark[3], color)
    # PlotPaperGraph('randomAttr-Son-cost-result.csv', costmodel[1], benchmark[2], color)
    # PlotPaperGraph('randomQue-Son-cost-result.csv', costmodel[1], benchmark[3], color)

    # 绘制折线图(Time Cost Compare)
    # xaxis:['customer','lineitem',...]
    # data Cost
    # name：文件名
    # xlabel：x轴名称
    # ylabel：y轴名称
    # color：线条颜色
    # color = SetupMplParams("Pastel1")
    # PlotLineChart('scvp-huang-sensitivity-result.csv','scvp-son-sensitivity-result.csv', color)
    PlotLineChart('scvp-random-huang-sensitivity-result.csv','scvp-random-son-sensitivity-result.csv', color)

    #绘制简单的柱状图
    # PlotSingleBar('tpch-Huang-reconstruction-join-cost.csv',costmodel[0],benchmark[0],'Avg tuple reconstruction joins')
    # PlotSingleBar('tpch-Huang-column-group-effectiveness.csv',costmodel[0],benchmark[0],'Avg column group effectiveness')
    # PlotSingleBar('tpch-Son-reconstruction-join-cost.csv', costmodel[1], benchmark[0],'Avg tuple reconstruction joins')
    # PlotSingleBar('tpch-Son-column-group-effectiveness.csv', costmodel[1], benchmark[0],'Avg column group effectiveness')
if __name__ == '__main__':
    Main()