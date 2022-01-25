import { Component } from "react";
import PropTypes from "prop-types";

class ScriptLoader extends Component {
  timeout = null;

  componentDidMount() {
    this.timeout = setTimeout(this._appendScript, this.props.delayMs);
  }

  componentWillUnmount() {
    clearTimeout(this.timeout);
  }

  _appendScript = () => {
    const { onCreate, onLoad, onError, delayMs, src, ...otherProps } =
      this.props;

    const script = document.createElement("script");
    script.src = src;

    // Add custom attributes
    for (const [attr, value] of Object.entries(otherProps)) {
      script.setAttribute(attr, value);
    }

    script.onload = onLoad;
    script.onerror = onError;
    document.body.appendChild(script);

    onCreate();
  };

  render() {
    return null;
  }
}

ScriptLoader.defaultProps = {
  delayMs: 0,
  onCreate: Function.prototype,
  onError: (e) => {
    throw new URIError(`The script ${e.target.src} is not accessible`);
  },
  onLoad: Function.prototype,
};

ScriptLoader.propTypes = {
  delayMs: PropTypes.number,
  onCreate: PropTypes.func,
  onError: PropTypes.func,
  onSuccess: PropTypes.func,
  src: PropTypes.string.isRequired,
};

export default ScriptLoader;
