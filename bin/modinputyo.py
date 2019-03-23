# this is the magic that fixes relative imports with the SDK
from __future__ import absolute_import

from splunklib.modularinput import *

import sys, os

class ModInputYoScript(Script):

    # get_scheme tells splunk (on start) what your input is all about.
    # it defines its name, how it's called (single instance), and what
    # arguments it accepts.
    def get_scheme(self):
        # user friendly name for this input, maybe what shows up in the UI?
        scheme = Scheme("Mod Input, yo")
        # it probably doesn't matter for your input, but use_single_instance = False
        # causes each defined input to call its own instance of this script
        scheme.use_single_instance = False

        # we'll make life a little easier by defining a list of arguments
        argument_names = ["start", "increment", "end"]

        # and then iterating through them to add to the scheme
        for argument_name in argument_names:
            argument = Argument(argument_name)
            scheme.add_argument(argument)

        return scheme

    # this is where we define what to do to create events
    # ew = EventWriter
    def stream_events(self, inputs, ew):

        # when use_single_instance=True, there will be more than one input in inputs.
        # but when use_single_instance=False, there will always be only one input in inputs.
        # regardless, inputs is a dict of names/configs, so we just iterate through it
        for input_name, input_item in inputs.inputs.iteritems():
            # input_item is a dict of its argument names/values
            start       = int(input_item["start"])
            increment   = int(input_item["increment"])
            end         = int(input_item["end"])

            # range stops at end (exclusive) so we'll add one to count through end
            for value in range(start, end+1, increment):

                # the SDK provids the Event class to hold the event you want to write
                event = Event()
                # event.stanza is (i think) used for the source field
                event.stanza = input_name
                # event.data is _raw
                event.data = "Current value: {0}".format(value)

                ew.write_event(event)


# you can ignore this, it's just glue that tells python to run your class
if __name__ == "__main__":
    sys.exit(ModInputYoScript().run(sys.argv))
