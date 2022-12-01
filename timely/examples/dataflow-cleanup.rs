extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

struct DropCanary;

impl Drop for DropCanary {
    fn drop(&mut self) {
        println!("dataflow dropped");
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let input_stream = scope.input_from(&mut input);

            let mut builder = OperatorBuilder::new("foobar".to_owned(), scope.clone());

            let mut input_handle = builder.new_input(&input_stream, Pipeline);
            let (_output_handle, output_stream) = builder.new_output::<usize>();

            let canary = DropCanary;
            builder.build_reschedule(move |_capabilities| move |_frontiers| {
                // Move the canary into the operator state
                let _canary = &canary;

                // If you comment out this line the dataflow is never dropped
                // input_handle.for_each(|_, _| {});

                false
            });

            output_stream.probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
