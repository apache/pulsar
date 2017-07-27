#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'yaml'

TERMS = YAML.load(File.open("_data/popovers.yaml"))

module Jekyll
  class RenderPopover < Liquid::Tag
    def initialize(tag_name, text, tokens)
      super

      @original_term = text.strip.split(' ').join(' ')
      @term = @original_term.gsub(' ', '-').downcase

      if ['acknowledged', 'acknowledgement', 'acknowledgements'].include? @term
        @term = 'ack'
      end

      if ['persisted', 'persists'].include? @term
        @term = 'persistent'
      end

      if @term == 'multi-tenant'
        @term = 'multi-tenancy'
      end

      if @term == 'properties'
        @term = 'property'
      end

      if @term.end_with? 's'
        @term = @term[0...-1]
      end

      if ['acknowledge', 'acknowledges', 'acknowledged'].include? @term
        @term = 'ack'
      end

      d = TERMS[@term]
      @definition, @question = d['def'], d['q']

      if @definition == nil
        abort("No definition for the term #{@term} found")
      end
    end

    def render(context)
      return "<span class=\"popover-term\" tabindex=\"0\" title=\"#{@question}\" data-placement=\"top\" data-content=\"#{@definition.strip}\" data-toggle=\"popover\" data-trigger=\"focus\">#{@original_term.strip}</span>"
    end
  end

  class JavadocUrl < Liquid::Tag
    def initialize(tag_name, text, tokens)
      args = text.split(' ')
      @name, @domain, @url = args[0], args[1], args[2].split('.')
    end

    def render(context)
      element = "<a target=\"_blank\" href=\"/api/#{@domain}/#{@url[0..-2].join('/')}/#{@url[-1]}.html\"><code class=\"highlighter-rouge\">#{@name}</code></a>"
      return element
    end
  end

  class RenderEndpointTag < Liquid::Tag
    def initialize(tag_name, markup, tokens)
      # split the markup contained in the tag into two arguments
      args = markup.split(" ")
      if args.length == 1
        @method, @url = "", args[0]
      else
        @method, @url = args[0], args[1]
      end
    end

    def render(context)
      modified_vars = @url.gsub(/{\w-}/, '<span class="arg">\0</span>')
      modified_url = @url.gsub(/:[\w-]+/, '<span class="endpoint">\0</span>')
      return "<div class=\"highlighter-rouge endpoint\"><pre class=\"highlight\"><code class=\"method #{@method.downcase}\">#{@method.upcase}</code><code class=\"url\">#{modified_url}</code></pre></div>"
    end
  end
end

Liquid::Template.register_tag('popover', Jekyll::RenderPopover)
Liquid::Template.register_tag('javadoc', Jekyll::JavadocUrl)
Liquid::Template.register_tag('endpoint', Jekyll::RenderEndpointTag)
