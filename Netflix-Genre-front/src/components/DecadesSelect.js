import React from 'react'
import {Select} from 'antd'

const Option = Select.Option

const decades = ['From the 1990s', 'From the 1980s', 'From the 1970s', 'From the 1960s', 'From the 1950s', 'Fom the 1940s', 'From the 1930s', 'From the 1920s', 'From the 1910s'];

class DecadesSelect extends React.Component {
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
                placeholder="选择 Decades"
                allowClear={true}
                onChange={this.handleChange}
            >
                {decades.map((decade) =>
                    <Option key={decade}>{decade}</Option>
                )}
            </Select>
        )
    }
}

export default DecadesSelect