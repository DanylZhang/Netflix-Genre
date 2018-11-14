import React from 'react'
import {Button, Icon, Table} from "antd";

const MoviesTable = ({movies, onClickSearch}) => {
    const columns = [{
        title: '影片ID',
        dataIndex: 'movie_id',
        key: 'movie_id',
        align: 'center',
        render: movie_id => (
            <a href={'https://www.netflix.com/watch/' + movie_id} target="_blank">{movie_id}</a>
        ),
    }, {
        title: '影片名',
        dataIndex: 'movie_en',
        key: 'movie_en',
        align: 'left',
        render: (text, record) => (
            <a href={'https://www.netflix.com/watch/' + record.movie_id} target="_blank">
                <img src={record.img_en} alt={record.movie_en}
                     style={{width: '80px', height: '45px', borderRadius: '5%'}}/>
                <span style={{margin: '5px'}}>
            {record.movie_en}
            </span>
            </a>
        ),
    }, {
        title: '影片名(港)',
        dataIndex: 'movie_cn',
        key: 'movie_cn',
        align: 'left',
        render: (text, record) => (
            <a href={'https://www.netflix.com/watch/' + record.movie_id} target="_blank">
                <img src={record.img_cn} alt={record.movie_cn}
                     style={{width: '80px', height: '45px', borderRadius: '5%'}}/>
                <span style={{margin: '5px'}}>
            {record.movie_cn}
            </span>
            </a>
        ),
    }, {
        title: '关联流派(港)',
        dataIndex: 'movie_genres',
        key: 'movie_genres',
        align: 'center',
        render: (text, record) => (
            <div>
                {
                    parseInt(record.movie_genres) > 0 ?
                        <Button type="primary" onClick={() => {
                            onClickSearch(record.movie_id)
                        }}>
                            <Icon type="left"/>{record.movie_genres}个
                        </Button> : '无'
                }
            </div>
        )
    }];

    return (
        <Table dataSource={movies} columns={columns} pagination={{pageSize: 5}} bordered={true}/>
    )
}

export default MoviesTable