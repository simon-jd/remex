module fifo_flaky (
  input logic clk, reset, push, pop, inject_fault,
  input logic [7:0] in_data,
  output logic [7:0] out_data,
  output logic full, empty
);
  // The history generator enables this simultaneous push/pop bug at revision 80.
  localparam bit PLANTED_BUG = 1'b0;
  logic [7:0] mem [4];
  logic [1:0] rd, wr;
  logic [2:0] count;
  assign empty = count == 0;
  assign full = count == 4;
  assign out_data = mem[rd];
  wire take_push = push && !full;
  wire take_pop = pop && !empty;
  always_ff @(posedge clk) begin
    if (reset) begin rd <= 0; wr <= 0; count <= 0; end
    else begin
      if (take_push) begin mem[wr] <= in_data; wr <= wr + 1'b1; end
      if (take_pop) rd <= rd + 1'b1;
      case ({take_push, take_pop})
        2'b10: count <= count + 1'b1;
        2'b01: count <= count - 1'b1;
        2'b11: if (PLANTED_BUG && inject_fault) count <= count + 1'b1;
        default: ;
      endcase
    end
  end
endmodule
