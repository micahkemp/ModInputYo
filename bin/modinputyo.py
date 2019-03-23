# this is the magic that fixes relative imports with the SDK
from __future__ import absolute_import

from splunklib.modularinput import *
import json
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
        argument_names = ["start", "increment", "end", "max_per_run"]

        # and then iterating through them to add to the scheme
        for argument_name in argument_names:
            argument = Argument(argument_name)
            scheme.add_argument(argument)

        return scheme

    # this is where we define what to do to create events
    # ew = EventWriter
    def stream_events(self, inputs, ew):

        # splunk gives us this by default.  save it in self to make life easier
        self.checkpoint_dir = inputs.metadata["checkpoint_dir"]

        # when use_single_instance=True, there will be more than one input in inputs.
        # but when use_single_instance=False, there will always be only one input in inputs.
        # regardless, inputs is a dict of names/configs, so we just iterate through it
        for input_stanza, input_item in inputs.inputs.iteritems():
            # input_item is a dict of its argument names/values
            start       = int(input_item["start"])
            increment   = int(input_item["increment"])
            end         = int(input_item["end"])
            max_per_run = int(input_item["max_per_run"])

            # get the actual input name, instead of the full stanza
            input_stanza_parts = input_stanza.split("/")
            # the stanza "modinputyo://some/name, when split by "/", contains these parts:
            #
            # modinputyo:
            # <empty> (between //)
            # some
            # name
            #
            # to get everything after the empty back in its original form:
            input_name = "/".join(input_stanza_parts[2:])

            # we'll start at the last_handled value, or the very beginning if no checkpoint yet
            starting_value = self.get_checkpoint_value(input_name, "last_handled") or start

            # iterate through max_per_run offsets
            for offset in range(1, max_per_run+1, increment):

                # calculate the current value as "where we started/left off" plus "how far we are in this run"
                current_value = starting_value + offset

                # nothing left to do when we reach the end
                if current_value>end:
                    break

                # the SDK provids the Event class to hold the event you want to write
                event = Event()
                # event.stanza is used for the source field (i'm no longer convinced of this, so it's commented out for now)
                #event.stanza = input_name
                # event.data is _raw
                event.data = "Current value: {0}".format(current_value)

                ew.write_event(event)

                # save the checkpoint after each event is written to avoid dupulication
                self.update_checkpoint(input_name, {"last_handled": current_value})

    def checkpoint_filename(self, input_name):
        # this assumes input_name will always be an acceptable filename for simplicity
        # if that is not the case input_name needs to be sanitized somehow
        return os.path.join(self.checkpoint_dir, input_name)

    # this checkpointing stuff was somewhat stolen/bastardized from
    # https://github.com/JasonConger/Splunk-REST-example/tree/master/TA_rest-example
    # remove (if exists) and write new state
    def update_checkpoint(self, input_name, state):

        checkpoint_filename = self.checkpoint_filename(input_name)

        if os.path.exists(checkpoint_filename):
            os.remove(checkpoint_filename)

        with open(checkpoint_filename, "w") as checkpoint_file:
            json.dump(state, checkpoint_file)

    def get_checkpoint(self, input_name):

        checkpoint_filename = self.checkpoint_filename(input_name)

        if os.path.exists(checkpoint_filename):
            with open(checkpoint_filename, "r") as checkpoint_file:
                return json.load(checkpoint_file)


    def get_checkpoint_value(self, input_name, key):

        checkpoint_contents = self.get_checkpoint(input_name)
        if checkpoint_contents:
            return checkpoint_contents[key]

# you can ignore this, it's just glue that tells python to run your class
if __name__ == "__main__":
    sys.exit(ModInputYoScript().run(sys.argv))
