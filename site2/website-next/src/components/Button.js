import React from "react";

class Button extends React.Component {
  render() {
    let classes = `p-2.5 cursor-pointer text-primary font-medium border border-primary rounded hover:bg-primary hover:text-white ${this.props.className}`;
    return (
      <div className={classes}>
        <a href={this.props.href} target={this.props.target}>
          {this.props.children}
        </a>
      </div>
    );
  }
}

Button.defaultProps = {
  target: "_self",
};

export default Button;
