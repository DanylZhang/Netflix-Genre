import React from 'react'
import {Select} from 'antd'

const Option = Select.Option

const descriptions = ['Absurd', 'Alien', 'Animal', 'Art', 'Assassination', 'Biographical', 'Bollywood', 'Bounty-Hunter', 'Buddy', 'Campy', 'Chilling', 'Classic', 'Coming-of-age', 'Conspiracy', 'Controversial', 'Cop', 'Courtroom', 'Critically-acclaimed', 'Cult', 'Cynical', 'Dark', 'Deadpan', 'Deep Sea', 'Detective', 'Disney', 'Dysfunctional-Family', 'Emotional', 'Epic', 'Experimental', 'Feel-good', 'Fight-the-System', 'Gambling', 'Gangster', 'Girl Power', 'Golden Globe Award-winning', 'Goofy', 'Gory', 'Gritty', 'Haunted House', 'Heartfelt', 'Heist', 'Imaginative', 'Independent', 'Inspiring', 'Irreverent', 'Kung Fu', 'Mad-Scientist', 'Magical', 'Martial Arts', 'Medical', 'Mid-Life-Crisis', 'Military', 'Mistaken-Identity', 'Monster', 'Ominous', 'Political', 'Prison', 'Provocative', 'Psychological', 'Quirky', 'Raunchy', 'Road Trip', 'Rogue-Cop', 'Romantic', 'Scary', 'Sentimental', 'Serial Killer', 'Slapstick', 'Social Issue', 'Space-Travel', 'Spy', 'Steamy', 'Supernatural', 'Suspenseful', 'Talking-Animal', 'Tearjerkers', 'Teen', 'Time Travel', 'Travel', 'Treasure Hunt', 'Underdog', 'Understated', 'Vampire', 'Violent', 'Viral Plague', 'Visually-striking', 'War', 'Werewolf', 'Witty', 'Workplace', 'Zombie'];

class DescriptionsSelect extends React.Component {
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
                placeholder="选择 Descriptions"
                allowClear={true}
                onChange={this.handleChange}
            >
                {descriptions.map((description) =>
                    <Option key={description}>{description}</Option>
                )}
            </Select>
        )
    }
}

export default DescriptionsSelect