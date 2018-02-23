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

TERMS_JA = YAML.load(File.open("_data/popovers-ja.yaml"))

module Jekyll
  class RenderPopoverJa < Liquid::Tag
    def initialize(tag_name, text, tokens)
      super

      @original_term = text.strip.split(' ').join(' ')
      @term = @original_term.gsub(' ', '-').downcase

      d = TERMS_JA[@term]
      @definition, @question = d['def'], d['q']

      if @definition == nil
        abort("No definition for the term #{@term} found")
      end
    end

    def render(context)
      return "<span class=\"popover-term\" tabindex=\"0\" title=\"#{@question}\" data-placement=\"top\" data-content=\"#{@definition.strip}\" data-toggle=\"popover\" data-trigger=\"focus\">#{@original_term.strip}</span>"
    end
  end
end

Liquid::Template.register_tag('popover_ja', Jekyll::RenderPopoverJa)
