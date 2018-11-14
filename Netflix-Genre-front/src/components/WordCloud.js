import React from 'react';
import _ from 'lodash';
import echarts from 'echarts/lib/echarts';
import 'echarts-wordcloud/index';

class WordCloud extends React.Component {
    constructor(props) {
        super(props)
    }

    componentDidMount() {
        // 初始化echarts对象
        this.initChart();
        // 响应式resize
        window.addEventListener('resize', () => {
            this.myChart.resize()
        })
        // 词云 onclick为 Netflix search
        this.myChart.on('click', (params) => {
            window.open('https://www.netflix.com/search?q=' + params.name)
        })
    }

    componentDidUpdate(preProps) {
        let {data} = this.props
        if (JSON.stringify(preProps.data) !== JSON.stringify(data)) {
            this.setChartOption(this.props);
        }
    }

    initChart() {
        let {mask} = this.props
        this.maskImage = new Image();
        this.maskImage.src = mask;
        this.myChart = echarts.init(this.refs.myChart)
    }

    setChartOption(nextProps) {
        let {data} = this.props
        if (!data) {
            return;
        }
        let _data = [];
        _.map(data, (item) => {
            let obj = {};
            obj.name = item[0];
            obj.value = parseInt(item[1] * 1000)
            _data.push(obj);
        })

        this.myChart.setOption({
            backgroundColor: '#fff',
            tooltip: {
                show: true
            },
            toolbox: {
                feature: {
                    saveAsImage: {
                        iconStyle: {
                            normal: {
                                color: '#000000'
                            }
                        }
                    }
                }
            },
            series: [{
                type: 'wordCloud',
                gridSize: 1,

                sizeRange: [1, 900],
                rotationRange: [0, 0],
                maskImage: this.maskImage,
                textStyle: {
                    normal: {
                        color: function (v) {
                            let color = ['#27D38A', '#FFCA1C', '#5DD1FA', '#F88E25', '#47A0FF', '#FD6565']
                            let num = Math.floor(Math.random() * (5 + 1));
                            return color[num];
                        },
                    },
                },
                left: 'center',
                top: 'center',
                width: '95%',
                height: '95%',
                // right: null,
                // bottom: null,
                // width: 300,
                // height: 200,
                // top: 20,
                data: _data
            }]
        })
    }

    render() {
        let {width, height} = this.props
        return (
            <div ref='myChart' style={{width: width, height: height}}>.</div>
        )
    }
}

export default WordCloud