import React, {Component} from 'react';
import './App.css';

import Axios from 'axios';
import {Col, Form, Input, Layout, Row} from 'antd';
import 'antd/dist/antd.css';
import GenresSelect from "./components/GenresSelect";
import DescriptionsSelect from "./components/DescriptionsSelect";
import DecadesSelect from "./components/DecadesSelect";
import GenresTable from "./components/GenresTable";
import MoviesTable from "./components/MoviesTable";
import WordCloud from "./components/WordCloud";
import {genreMask, movieMask, wechatPay, zhifubaoPay} from './const/imageBase64';
import _ from 'lodash';
import RelationGraph from "./components/RelationGraph";

const FormItem = Form.Item;
const Search = Input.Search;
const {Header, Content, Footer} = Layout

class App extends Component {
    constructor(props) {
        super(props)
        this.state = {
            reply: undefined,
            movieReply: undefined,
            genreTags: undefined,
            movieTags: undefined,
            relationData: undefined,
        }

        this.genres = []
        this.descriptions = []
        this.decades = []
        this.genresSelect = this.genresSelect.bind(this);
        this.descriptionsSelect = this.descriptionsSelect.bind(this);
        this.decadesSelect = this.decadesSelect.bind(this);
        this.doGenreSearch = this.doGenreSearch.bind(this);
    }

    genresSelect(searchArray) {
        this.genres = searchArray;
    }

    descriptionsSelect(searchArray) {
        this.descriptions = searchArray;
    }

    decadesSelect(searchArray) {
        this.decades = searchArray;
    }

    doGenreSearch(value, type) {
        if (type === 'movie_id') {
            this.getJsonData({
                movie_id: value
            })
        } else {
            this.getJsonData({
                genres: this.genres,
                descriptions: this.descriptions,
                decades: this.decades,
                search: _.filter([value])
            })
        }
    }

    doMovieSearch(value, type) {
        if (type === 'genre_id') {
            this.getMovieJsonData({
                genre_id: value
            })
        } else {
            this.getMovieJsonData({
                search: value
            })
        }
    }

    componentDidMount() {
        this.getJsonData();
        this.getMovieJsonData();
        this.getGenreTagsData();
        this.getMovieTagsData();
    }

    // search table data
    getJsonData = (searchObj) => {
        Axios.post("/v1/genre/search", searchObj).then((response) => {
            response = response.data
            this.setState({reply: response})
        }).catch((error) => {
            console.log(error);
        })
    }
    getMovieJsonData = (searchObj) => {
        Axios.post("/v1/movie/search", searchObj).then((response) => {
            response = response.data
            this.setState({movieReply: response})
            this.getRelationData(_.take(_.map(response, (item) => item.movie_id), 30));
        }).catch((error) => {
            console.log(error);
        })
    }

    // word cloud data
    getGenreTagsData = () => {
        Axios.get("/v1/genre/wordcount").then((response) => {
            response = response.data
            this.setState({genreTags: response})
        }).catch((error) => {
            console.log(error)
        })
    }
    getMovieTagsData = () => {
        Axios.get("/v1/movie/wordcount").then((response) => {
            response = response.data
            this.setState({movieTags: response})
        }).catch((error) => {
            console.log(error)
        })
    }

    // relation data
    getRelationData(movieIds) {
        Axios.post("/v1/movie/genre_movie_relation", movieIds).then((response) => {
            response = response.data
            this.setState({relationData: response})
        }).catch((error) => {
            console.log(error)
        })
    }

    render() {
        return (
            <Layout>
                <Header style={{position: 'fixed', zIndex: 1, width: '100%'}}>
                    <h1 style={{color: 'white'}}>Netflix Genre</h1>
                </Header>
                <Content style={{padding: '0 10px', marginTop: 64}}>
                    <div style={{background: '#fff', padding: 8, minHeight: 380}}>
                        {/*Search Table*/}
                        <Row gutter={16}>
                            <Col xs={24} sm={24} md={12} lg={12} xl={12}>
                                <span><h5>Genre Search</h5></span>
                                <Form layout="inline">
                                    <FormItem>
                                        <GenresSelect onSelectChange={this.genresSelect}/>
                                    </FormItem>

                                    <FormItem>
                                        <DescriptionsSelect onSelectChange={this.descriptionsSelect}/>
                                    </FormItem>

                                    <FormItem>
                                        <DecadesSelect onSelectChange={this.decadesSelect}/>
                                    </FormItem>

                                    <FormItem>
                                        <Search
                                            placeholder="自定义搜索"
                                            onSearch={value => this.doGenreSearch(value)}
                                            enterButton
                                        />
                                    </FormItem>
                                </Form>
                                <hr/>
                                <GenresTable genres={this.state.reply} onClickSearch={(genre_id) => {
                                    this.doMovieSearch(genre_id, 'genre_id')
                                }}/>
                            </Col>
                            <Col xs={24} sm={24} md={12} lg={12} xl={12}>
                                <span><h5>Movie Search</h5></span>
                                <Form layout="inline">
                                    <FormItem>
                                        <Search
                                            placeholder="自定义搜索"
                                            onSearch={value => this.doMovieSearch(value)}
                                            enterButton
                                        />
                                    </FormItem>
                                </Form>
                                <hr/>
                                <MoviesTable movies={this.state.movieReply} onClickSearch={(movie_id) => {
                                    this.doGenreSearch(movie_id, 'movie_id')
                                }}/>
                            </Col>
                        </Row>

                        {/*echarts ciyun*/}
                        <Row gutter={16}>
                            <Col xs={24} sm={24} md={12} lg={12} xl={12}>
                                <span><h5>Genre Word Cloud</h5></span>
                                <WordCloud mask={genreMask} data={this.state.genreTags} width={'100%'} height={500}/>
                            </Col>
                            <Col xs={24} sm={24} md={12} lg={12} xl={12}>
                                <span><h5>Movie Word Cloud</h5></span>
                                <WordCloud mask={movieMask} data={this.state.movieTags} width={'100%'} height={500}/>
                            </Col>
                        </Row>

                        {/*echarts relation-graph*/}
                        <Row gutter={16}>
                            <span><h5>Genre与Movie关系图</h5></span>
                            <RelationGraph data={this.state.relationData} width={'100%'} height={600}/>
                        </Row>

                        {/*vpn*/}
                        <Row gutter={16}>
                            <Col xs={24} sm={24} md={8} lg={8} xl={8}>
                                <div style={{color: 'grey'}}>推荐高可用VPN:
                                    &nbsp;&nbsp;<a href='https://www.expressvpn.com/order' target='_blank'>Express
                                        VPN</a>
                                </div>
                            </Col>
                            <Col xs={24} sm={24} md={8} lg={8} xl={8}>
                                <h5 align="center">Buy me a coffee(微信)</h5>
                                <img align="center" src={wechatPay} style={{width: '300px', height: '260px'}}></img>
                            </Col>
                            <Col xs={24} sm={24} md={8} lg={8} xl={8}>
                                <h5 align="center">Buy me a coffee(支付宝)</h5>
                                <img align="center" src={zhifubaoPay} style={{width: '300px', height: '300px'}}></img>
                            </Col>
                        </Row>
                    </div>
                </Content>
                <Footer style={{textAlign: 'center'}}>
                    Netflix Genre ©2018 Created by Danyl
                </Footer>
            </Layout>
        );
    }
}

export default App;