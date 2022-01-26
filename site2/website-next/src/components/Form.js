import React, { Component } from "react";



class Form extends Component {
  initialState = {
    searchString: ''
  }
  state = this.initialState;

  handleChange = (event) => {
    const {name, value} = event.target;
    this.setstate({
      [name]: value
    });
  }
  render() {
    const { searchString } = this.state;
  
    return (
      <form className="relative z10">
        <label htmlFor="searchString">Search</label>
        <input
          type="text"
          name="searchString"
          id="searchString"
          value={searchString}
          onChange={this.handleChange} />
      </form>
    );
  }

}
export default Form;