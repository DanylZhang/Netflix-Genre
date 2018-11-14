import React from 'react';
import _ from 'lodash';
import echarts from 'echarts/lib/echarts';
import 'echarts/lib/chart/graph';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';

class RelationGraph extends React.Component {
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
    }

    componentDidUpdate(preProps) {
        let {data} = this.props
        if (JSON.stringify(preProps.data) !== JSON.stringify(data)) {
            this.setChartOption(this.props);
        }
    }

    initChart() {
        this.myChart = echarts.init(this.refs.myChart)
    }

    setChartOption(nextProps) {
        let {data} = this.props
        if (!data) {
            return;
        }
        let genre_symbolSize = _.groupBy(data, (item) => item.genre_id)
        let movie_symbolSize = _.groupBy(data, (item) => item.movie_id)
        let nodes = _.flatMap(data, (item) => {
            let genre = {
                name: item.genre_en,
                value: item.genre_id,
                symbolSize: genre_symbolSize[item.genre_id].length,
                category: 'genre',
                // Use random x, y
                x: null,
                y: null,
                draggable: true,
            }
            let movie = {
                name: item.movie_en,
                value: item.movie_id,
                symbol: 'image://' + item.img_en,
                symbolSize: [movie_symbolSize[item.movie_id].length, parseInt(movie_symbolSize[item.movie_id].length * 0.5)],
                category: 'movie',
                x: null,
                y: null,
                draggable: true,
            }
            return [genre, movie]
        })
        // fix: Cannot set property 'dataIndex' of undefined, because data nodes duplicated
        nodes = _.uniqBy(nodes, (item) => item.name)

        let links = _.map(data, (item) => {
            return {
                source: item.movie_en,
                target: item.genre_en
            }
        })

        this.myChart.setOption({
            title: {
                text: '',// 'Genre与Movie关系图',
                subtext: '',// 'Netflix Genre 与 Movies之间的关系',
                top: 'top',
                left: 'center'
            },
            tooltip: {},
            animationDuration: 1500,
            animationEasingUpdate: 'quinticInOut',
            series: [{
                name: 'Genre与Movie关系图',
                type: 'graph',
                layout: 'force',
                data: nodes,
                links: links,
                categories: ['genre', 'movie'],
                roam: true,
                focusNodeAdjacency: true,
                itemStyle: {
                    normal: {
                        borderColor: '#fff',
                        borderWidth: 1,
                        shadowBlur: 10,
                        shadowColor: 'rgba(0, 0, 0, 0.3)'
                    }
                },
                label: {
                    position: 'right',
                    formatter: '{b}'
                },
                lineStyle: {
                    color: 'source',
                    curveness: 0.3
                },
                force: {
                    repulsion: 100
                },
                emphasis: {
                    lineStyle: {
                        width: 10
                    }
                }
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

export default RelationGraph