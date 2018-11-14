import React from 'react'
import {Button, Icon, Table} from "antd"

const GenresTable = ({genres, onClickSearch}) => {
    const columns = [{
        title: '流派ID',
        dataIndex: 'genre_id',
        key: 'genre_id',
        align: 'center',
        render: genre_id => (
            <a href={'https://www.netflix.com/browse/genre/' + genre_id} target="_blank">{genre_id}</a>
        ),
    }, {
        title: '流派',
        dataIndex: 'genre_en',
        key: 'genre_en',
        align: 'center',
        render: (text, record) => (
            <a href={'https://www.netflix.com/browse/genre/' + record.genre_id} target="_blank">{record.genre_en}</a>
        ),
    }, {
        title: '流派(港)',
        dataIndex: 'genre_cn',
        key: 'genre_cn',
        align: 'center',
        render: (text, record) => (
            <a href={'https://www.netflix.com/browse/genre/' + record.genre_id} target="_blank">{record.genre_cn}</a>
        ),
    }, {
        title: '相关影片(港)',
        dataIndex: 'genre_movies',
        key: 'genre_movies',
        align: 'center',
        render: (text, record) => (
            <div>
                {
                    parseInt(record.genre_movies) > 0 ?
                        <Button type="primary" onClick={() => {
                            onClickSearch(record.genre_id)
                        }}>
                            {record.genre_movies}个<Icon type="right"/>
                        </Button> : '无'
                }
            </div>
        )
    }];

    return (
        <Table dataSource={genres} columns={columns} pagination={{pageSize: 5}} bordered={true}/>
    )
}

export default GenresTable