import React from 'react'
import {Select} from 'antd'

const Option = Select.Option

const genres = ['Action & Adventure', 'Anime', 'Children & Family', 'Classic', 'Comedies', 'Cult', 'Documentaries', 'Dramas', 'Faith & Sprituality', 'Foreign', 'Gay & Lesbian', 'Horror', 'Independent', 'Music', 'Musicals', 'Romantic', 'Sci-Fi & Fantasy', 'Sports', 'Thrillers', 'TV Shows'];

class GenresSelect extends React.Component {
    constructor(props) {
        super(props)
        this.handleChange = this.handleChange.bind(this)
    }

    handleChange(value) {
        this.props.onSelectChange(value)
    }

    render() {
        return (
            <Select
                mode="multiple"
                style={{width: 200}}
                placeholder="选择 Genres"
                allowClear={true}
                onChange={this.handleChange}
            >
                {genres.map((genre) =>
                    <Option key={genre}>{genre}</Option>
                )}
            </Select>
        )
    }
}

export default GenresSelect